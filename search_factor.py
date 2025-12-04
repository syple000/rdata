import subprocess
import re
import csv
import os
import argparse
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

# Kline因子类型
KLINE_FACTOR_TYPES = [
    "PriceReturn",
    "TrendStrength",
    "PriceVolatility",
    "PriceRange",
    "PricePosition",
    "AvgVolume",
    "VolumeVolatility",
    "VolumeTrend",
    "OBV",
    "PriceVolumeCorrelation",
    "AvgIntradayRange",
    "AvgBodyRatio",
]

# Trade因子类型
TRADE_FACTOR_TYPES = [
    "PriceReturn",
    "TrendStrength",
    "PriceVolatility",
    "PriceRange",
    "PriceAcceleration",
    "PricePosition",
    "AvgVol",
    "VolVolatility",
    "VolSkew",
    "LargeTradeRatio",
    "VolTrend",
    "BuyCount",
    "SellCount",
    "TradeImbalance",
    "BuyVol",
    "SellVol",
    "VolImbalance",
    "NetBuyRatio",
    "AvgTradeSizeRatio",
    "Vwap",
    "PriceVwapDeviation",
    "VwapSlope",
    "OBV",
    "PriceVolumeCorrelation",
    "TradeFrequency",
    "AvgTradeInterval",
    "TradeIntervalStd",
]

# 参数配置
@dataclass
class SearchConfig:
    # 时间范围
    from_ts: int = 1609430400000  # 2021-01-01
    to_ts: int = 1761926400000    # 2025-11-01
    
    # 市场配置
    market_type: str = "binance_spot"
    symbol: str = "BTCUSDT"
    
    # Kline特有参数
    kline_intervals: Optional[List[str]] = None  # ["1m", "1s"]
    
    # 通用参数
    window_sizes: Optional[List[int]] = None     # [60, 120, 300, 600]
    step_ms_list: Optional[List[int]] = None     # [60000, 300000]
    forward_steps_list: Optional[List[int]] = None  # [10, 30, 60]
    
    # 数据类型
    data_types: Optional[List[str]] = None       # ["kline", "trade"]
    
    # 执行配置
    platform_path: str = "./platform/target/release/platform"
    output_file: str = "factor_ic_results.csv"
    max_workers: int = 1  # 并行执行的worker数量
    
    def __post_init__(self):
        if self.kline_intervals is None:
            self.kline_intervals = ["1m"]
        if self.window_sizes is None:
            self.window_sizes = [60, 120, 300]
        if self.step_ms_list is None:
            self.step_ms_list = [60000]
        if self.forward_steps_list is None:
            self.forward_steps_list = [10, 30, 60]
        if self.data_types is None:
            self.data_types = ["kline", "trade"]


@dataclass
class FactorResult:
    data_type: str
    factor_type: str
    interval: Optional[str]  # kline only
    window_size: int
    step_ms: int
    forward_steps: int
    market_type: str
    symbol: str
    ic: Optional[float]
    records: Optional[int]
    status: str  # "success", "failed", "error"
    error_message: Optional[str]


def build_command(
    platform_path: str,
    from_ts: int,
    to_ts: int,
    data_type: str,
    factor_type: str,
    interval: Optional[str],
    window_size: int,
    market_type: str,
    symbol: str,
    step_ms: int,
    forward_steps: int,
) -> List[str]:
    """构建执行命令"""
    cmd = [
        platform_path,
        "--command", "factor_backtest",
        "--from_ts", str(from_ts),
        "--to_ts", str(to_ts),
        "--data_type", data_type,
        "--factor_type", factor_type,
        "--window_size", str(window_size),
        "--market_type", market_type,
        "--symbol", symbol,
        "--step_ms", str(step_ms),
        "--forward_steps", str(forward_steps),
    ]
    if data_type == "kline" and interval:
        cmd.extend(["--interval", interval])
    return cmd


def parse_ic_from_log(log_file: str) -> tuple[Optional[float], Optional[int]]:
    """从日志文件解析IC值和记录数"""
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # 匹配: factor backtest finished, records: 123, IC: 0.123456
        pattern = r"factor backtest finished, records: (\d+), IC: ([-\d.]+)"
        match = re.search(pattern, content)
        if match:
            records = int(match.group(1))
            ic = float(match.group(2))
            return ic, records
    except Exception as e:
        print(f"Error parsing log file {log_file}: {e}")
    return None, None


def run_single_backtest(
    platform_path: str,
    from_ts: int,
    to_ts: int,
    data_type: str,
    factor_type: str,
    interval: Optional[str],
    window_size: int,
    market_type: str,
    symbol: str,
    step_ms: int,
    forward_steps: int,
    work_dir: str,
) -> FactorResult:
    """运行单个因子回测"""
    cmd = build_command(
        platform_path, from_ts, to_ts, data_type, factor_type,
        interval, window_size, market_type, symbol, step_ms, forward_steps
    )
    
    result = FactorResult(
        data_type=data_type,
        factor_type=factor_type,
        interval=interval,
        window_size=window_size,
        step_ms=step_ms,
        forward_steps=forward_steps,
        market_type=market_type,
        symbol=symbol,
        ic=None,
        records=None,
        status="pending",
        error_message=None,
    )
    
    try:
        # 日志文件路径
        log_file = os.path.join(work_dir, "platform_factor_backtest.log")
        if os.path.exists(log_file):
            os.remove(log_file)

        print(f"Running: {' '.join(cmd)}")
        process = subprocess.run(
            cmd,
            cwd=work_dir,
            capture_output=True,
            text=True,
            timeout=3600,  # 1小时超时
        )
        
        if os.path.exists(log_file):
            ic, records = parse_ic_from_log(log_file)
            if ic is not None:
                result.ic = ic
                result.records = records
                result.status = "success"
            else:
                result.status = "failed"
                result.error_message = "Could not parse IC from log"
        else:
            result.status = "failed"
            result.error_message = "Log file not found"
            
    except subprocess.TimeoutExpired:
        result.status = "error"
        result.error_message = "Timeout"
    except Exception as e:
        result.status = "error"
        result.error_message = str(e)
    
    return result


def generate_tasks(config: SearchConfig) -> List[dict]:
    """生成所有需要执行的任务"""
    tasks = []
    
    data_types = config.data_types or []
    window_sizes = config.window_sizes or []
    step_ms_list = config.step_ms_list or []
    forward_steps_list = config.forward_steps_list or []
    kline_intervals = config.kline_intervals or []
    
    for data_type in data_types:
        factor_types = KLINE_FACTOR_TYPES if data_type == "kline" else TRADE_FACTOR_TYPES
        
        for window_size in window_sizes:
            for step_ms in step_ms_list:
                for forward_steps in forward_steps_list:
                    for factor_type in factor_types:
                        if data_type == "kline":
                            for interval in kline_intervals:
                                tasks.append({
                                    "data_type": data_type,
                                    "factor_type": factor_type,
                                    "interval": interval,
                                    "window_size": window_size,
                                    "step_ms": step_ms,
                                    "forward_steps": forward_steps,
                                })
                        else:
                            tasks.append({
                                "data_type": data_type,
                                "factor_type": factor_type,
                                "interval": None,
                                "window_size": window_size,
                                "step_ms": step_ms,
                                "forward_steps": forward_steps,
                            })
    
    return tasks


def save_results(results: List[FactorResult], output_file: str):
    """保存结果到CSV文件"""
    fieldnames = [
        "data_type", "factor_type", "interval", "window_size",
        "step_ms", "forward_steps", "market_type", "symbol",
        "ic", "records", "status", "error_message"
    ]
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow({
                "data_type": result.data_type,
                "factor_type": result.factor_type,
                "interval": result.interval or "",
                "window_size": result.window_size,
                "step_ms": result.step_ms,
                "forward_steps": result.forward_steps,
                "market_type": result.market_type,
                "symbol": result.symbol,
                "ic": result.ic if result.ic is not None else "",
                "records": result.records if result.records is not None else "",
                "status": result.status,
                "error_message": result.error_message or "",
            })
    
    print(f"Results saved to {output_file}")


def run_search(config: SearchConfig):
    """运行因子搜索"""
    tasks = generate_tasks(config)
    total_tasks = len(tasks)
    print(f"Total tasks to run: {total_tasks}")
    
    results: List[FactorResult] = []
    work_dir = os.path.dirname(os.path.abspath(config.platform_path))
    
    for i, task in enumerate(tasks):
        print(f"\n[{i+1}/{total_tasks}] Processing {task['data_type']} - {task['factor_type']}")
        
        result = run_single_backtest(
            platform_path=config.platform_path,
            from_ts=config.from_ts,
            to_ts=config.to_ts,
            data_type=task["data_type"],
            factor_type=task["factor_type"],
            interval=task["interval"],
            window_size=task["window_size"],
            market_type=config.market_type,
            symbol=config.symbol,
            step_ms=task["step_ms"],
            forward_steps=task["forward_steps"],
            work_dir=work_dir,
        )
        
        results.append(result)
        
        # 实时打印结果
        if result.status == "success":
            print(f"  ✓ IC: {result.ic:.6f}, Records: {result.records}")
        else:
            print(f"  ✗ {result.status}: {result.error_message}")
        
        # 每10个任务保存一次中间结果
        if (i + 1) % 10 == 0:
            save_results(results, config.output_file)
    
    # 最终保存
    save_results(results, config.output_file)
    
    # 打印汇总统计
    print("\n" + "=" * 60)
    print("Summary:")
    success_count = sum(1 for r in results if r.status == "success")
    failed_count = sum(1 for r in results if r.status == "failed")
    error_count = sum(1 for r in results if r.status == "error")
    print(f"  Success: {success_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Error: {error_count}")
    
    # 打印IC值排名（前10）
    successful_results = [r for r in results if r.status == "success" and r.ic is not None]
    if successful_results:
        print("\nTop 10 IC values (absolute):")
        sorted_results = sorted(successful_results, key=lambda x: abs(x.ic or 0), reverse=True)[:10]
        for i, r in enumerate(sorted_results, 1):
            interval_str = f", interval={r.interval}" if r.interval else ""
            print(f"  {i}. {r.data_type}/{r.factor_type}{interval_str}, "
                  f"ws={r.window_size}, fs={r.forward_steps} -> IC: {r.ic:.6f}")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Search for factor IC values")
    
    parser.add_argument("--from_ts", type=int, default=1609430400000,
                        help="Start timestamp in milliseconds")
    parser.add_argument("--to_ts", type=int, default=1761926400000,
                        help="End timestamp in milliseconds")
    parser.add_argument("--market_type", type=str, default="binance_spot",
                        help="Market type")
    parser.add_argument("--symbol", type=str, default="BTCUSDT",
                        help="Trading symbol")
    parser.add_argument("--platform_path", type=str, 
                        default="./platform/target/release/platform",
                        help="Path to platform executable")
    parser.add_argument("--output", type=str, default="factor_ic_results.csv",
                        help="Output CSV file path")
    parser.add_argument("--data_types", type=str, nargs="+", 
                        default=["kline", "trade"],
                        help="Data types to test (kline, trade)")
    parser.add_argument("--intervals", type=str, nargs="+",
                        default=["1m"],
                        help="Kline intervals to test")
    parser.add_argument("--window_sizes", type=int, nargs="+",
                        default=[10, 30, 60, 120],
                        help="Window sizes to test")
    parser.add_argument("--step_ms_list", type=int, nargs="+",
                        default=[60000],
                        help="Step milliseconds to test")
    parser.add_argument("--forward_steps", type=int, nargs="+",
                        default=[1, 3, 5, 10, 30, 60],
                        help="Forward steps to test")
    
    return parser.parse_args()


# 搜索因子IC，找到符合预期的因子，数据区间，预测区间
if __name__ == '__main__':
    args = parse_args()
    
    config = SearchConfig(
        from_ts=args.from_ts,
        to_ts=args.to_ts,
        market_type=args.market_type,
        symbol=args.symbol,
        kline_intervals=args.intervals,
        window_sizes=args.window_sizes,
        step_ms_list=args.step_ms_list,
        forward_steps_list=args.forward_steps,
        data_types=args.data_types,
        platform_path=args.platform_path,
        output_file=args.output,
    )
    
    print("Factor Search Configuration:")
    print(f"  Time Range: {config.from_ts} - {config.to_ts}")
    print(f"  Market: {config.market_type} / {config.symbol}")
    print(f"  Data Types: {config.data_types}")
    print(f"  Kline Intervals: {config.kline_intervals}")
    print(f"  Window Sizes: {config.window_sizes}")
    print(f"  Step MS: {config.step_ms_list}")
    print(f"  Forward Steps: {config.forward_steps_list}")
    print(f"  Output: {config.output_file}")
    print()
    
    run_search(config)