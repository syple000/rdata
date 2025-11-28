# !/bin/bash

# 编译platform模块
cd platform && cargo build --release && cd -

# 创建build目录
mkdir -p build
mkdir -p build/conf

# 复制可执行文件到build目录
cp target/release/platform build/platform

# 复制配置文件到build目录
cp platform/conf/platform_conf.toml build/conf/platform_conf.toml