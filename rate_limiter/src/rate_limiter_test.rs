#[cfg(test)]
mod tests {
    use crate::{RateLimiter, RateLimiterError};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{sleep, Instant};

    #[tokio::test]
    async fn test_new_rate_limiter() {
        let _limiter = RateLimiter::new(Duration::from_millis(100), 10);
        // 测试创建成功，没有恐慌
        assert!(true);
    }

    #[tokio::test]
    async fn test_allow_basic_success() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试正常情况下允许请求
        assert!(limiter.allow(5).await.is_ok());
        assert!(limiter.allow(3).await.is_ok());
    }

    #[tokio::test]
    async fn test_allow_zero_weight_error() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试权重为0的错误情况
        let result = limiter.allow(0).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RateLimiterError::InvalidWeight { .. }
        ));
    }

    #[tokio::test]
    async fn test_allow_weight_exceeds_limit_error() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试权重超过最大限制的错误情况
        let result = limiter.allow(15).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RateLimiterError::InvalidWeight { .. }
        ));
    }

    #[tokio::test]
    async fn test_allow_exceed_limit() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 先添加权重8的请求
        assert!(limiter.allow(8).await.is_ok());

        // 再添加权重5的请求，应该超过限制
        let result = limiter.allow(5).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RateLimiterError::Limited));
    }

    #[tokio::test]
    async fn test_allow_exactly_at_limit() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试恰好达到限制
        assert!(limiter.allow(10).await.is_ok());

        // 再添加任何权重都应该失败
        let result = limiter.allow(1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RateLimiterError::Limited));
    }

    #[tokio::test]
    async fn test_window_cleanup() {
        let limiter = RateLimiter::new(Duration::from_millis(50), 10);

        // 添加权重10的请求
        assert!(limiter.allow(10).await.is_ok());

        // 立即再次尝试应该失败
        let result = limiter.allow(1).await;
        assert!(result.is_err());

        // 等待窗口时间过期
        sleep(Duration::from_millis(60)).await;

        // 现在应该可以再次添加请求
        assert!(limiter.allow(5).await.is_ok());
    }

    #[tokio::test]
    async fn test_wait_basic_success() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试正常情况下等待请求
        assert!(limiter.wait(5).await.is_ok());
        assert!(limiter.wait(3).await.is_ok());
    }

    #[tokio::test]
    async fn test_wait_zero_weight_error() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试权重为0的错误情况
        let result = limiter.wait(0).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RateLimiterError::InvalidWeight { .. }
        ));
    }

    #[tokio::test]
    async fn test_wait_weight_exceeds_limit_error() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试权重超过最大限制的错误情况
        let result = limiter.wait(15).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RateLimiterError::InvalidWeight { .. }
        ));
    }

    #[tokio::test]
    async fn test_wait_blocks_then_succeeds() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 先填满限制
        assert!(limiter.allow(10).await.is_ok());

        let start = Instant::now();

        // 等待应该会阻塞直到窗口清理
        assert!(limiter.wait(5).await.is_ok());

        let elapsed = start.elapsed();
        // 应该至少等待了100ms
        assert!(elapsed >= Duration::from_millis(90)); // 允许一些误差
    }

    #[tokio::test]
    async fn test_wait_exactly_at_limit() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 测试恰好达到限制的等待
        assert!(limiter.wait(10).await.is_ok());

        let start = Instant::now();

        // 等待另一个10权重的请求
        assert!(limiter.wait(10).await.is_ok());

        let elapsed = start.elapsed();
        // 应该至少等待了100ms
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_multiple_requests_within_window() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 在窗口内添加多个请求
        assert!(limiter.allow(2).await.is_ok());
        assert!(limiter.allow(3).await.is_ok());
        assert!(limiter.allow(4).await.is_ok());

        // 总权重为9，还可以添加1
        assert!(limiter.allow(1).await.is_ok());

        // 现在应该达到限制
        let result = limiter.allow(1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RateLimiterError::Limited));
    }

    #[tokio::test]
    async fn test_partial_window_cleanup() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 10);

        // 添加第一个请求
        assert!(limiter.allow(5).await.is_ok());

        // 等待一小段时间
        sleep(Duration::from_millis(30)).await;

        // 添加第二个请求
        assert!(limiter.allow(4).await.is_ok());

        // 现在总权重是9，还可以添加1
        assert!(limiter.allow(1).await.is_ok());

        // 等待第一个请求过期
        sleep(Duration::from_millis(80)).await;

        // 现在第一个请求应该已经清理，可以添加更多
        assert!(limiter.allow(5).await.is_ok());
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let limiter = Arc::new(RateLimiter::new(Duration::from_millis(100), 10));
        let mut handles = vec![];

        // 启动多个并发任务
        for _ in 0..5 {
            let limiter_clone: Arc<RateLimiter> = Arc::clone(&limiter);
            let handle = tokio::spawn(async move {
                let weight = 2;
                limiter_clone.allow(weight).await
            });
            handles.push(handle);
        }

        // 收集结果
        let mut success_count = 0;
        let mut error_count = 0;

        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(_) => error_count += 1,
            }
        }

        // 总权重限制是10，每个请求权重2
        // 所以最多5个请求可以成功，但由于并发，可能少一些
        assert!(success_count <= 5);
        assert!(success_count + error_count == 5);
        assert!(success_count >= 1); // 至少应该有一个成功
    }

    #[tokio::test]
    async fn test_concurrent_wait() {
        let limiter = Arc::new(RateLimiter::new(Duration::from_millis(100), 6));
        let mut handles = vec![];

        // 启动多个并发等待任务
        for _ in 0..3 {
            let limiter_clone: Arc<RateLimiter> = Arc::clone(&limiter);
            let handle = tokio::spawn(async move { limiter_clone.wait(3).await });
            handles.push(handle);
        }

        let start = Instant::now();

        // 等待所有任务完成
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        let elapsed = start.elapsed();

        // 由于需要等待窗口清理，总时间应该至少是一个窗口期
        // 第一个请求立即成功，后续请求可能需要等待
        assert!(elapsed >= Duration::from_millis(80)); // 允许一些误差，因为实际执行可能更快
    }

    #[tokio::test]
    async fn test_edge_case_single_weight_unit() {
        let limiter = RateLimiter::new(Duration::from_millis(50), 1);

        // 测试最小权重限制
        assert!(limiter.allow(1).await.is_ok());

        // 立即再次尝试应该失败
        let result = limiter.allow(1).await;
        assert!(result.is_err());

        // 等待窗口清理
        sleep(Duration::from_millis(60)).await;

        // 现在应该可以再次添加
        assert!(limiter.allow(1).await.is_ok());
    }

    #[tokio::test]
    async fn test_large_weight_values() {
        let limiter = RateLimiter::new(Duration::from_millis(100), 1000);

        // 测试大权重值
        assert!(limiter.allow(999).await.is_ok());
        assert!(limiter.allow(1).await.is_ok());

        // 现在应该达到限制
        let result = limiter.allow(1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), RateLimiterError::Limited));
    }

    #[tokio::test]
    async fn test_very_short_window() {
        let limiter = RateLimiter::new(Duration::from_millis(1), 5);

        // 填满限制
        assert!(limiter.allow(5).await.is_ok());

        // 等待很短时间
        sleep(Duration::from_millis(2)).await;

        // 应该可以再次添加
        assert!(limiter.allow(5).await.is_ok());
    }
}
