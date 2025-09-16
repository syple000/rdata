use std::time::Duration;
use time::get_current_nano_timestamp;
use tokio::sync::Mutex;

#[derive(Clone)]
struct Elem(u128, u64); // (timestamp, weight)

struct Inner {
    data: Vec<Elem>,
    start: usize,
    end: usize,
    size: usize,
    weight_sum: u64,
}

impl Inner {
    fn cleanup(&mut self, cutoff: u128) {
        for e in self.data.iter() {
            if e.0 < cutoff && self.size > 0 {
                self.weight_sum -= e.1;
                self.start = (self.start + 1) % self.data.len();
                self.size -= 1;
            } else {
                break;
            }
        }
    }
}

pub struct RateLimiter {
    max_window_range: Duration,
    max_weight_limit: u64,
    inner: Mutex<Inner>,
}

impl RateLimiter {
    // max_window_range: 单位nanoseconds
    // max_weight_limit: 窗口范围内的最大权重和
    pub fn new(max_window_range: Duration, max_weight_limit: u64) -> Self {
        Self {
            max_window_range,
            max_weight_limit,
            inner: Mutex::new(Inner {
                data: vec![Elem(0, 0); max_weight_limit as usize],
                start: 0,
                end: 0,
                size: 0,
                weight_sum: 0,
            }),
        }
    }

    pub async fn allow(&self, weight: u64) -> Result<(), String> {
        if weight == 0 {
            return Err("weight must be greater than 0".to_string());
        }
        if weight > self.max_weight_limit {
            return Err(format!(
                "weight must be less than or equal to max_weight_limit: {}",
                self.max_weight_limit
            ));
        }

        let mut inner = self.inner.lock().await;

        let timestamp = get_current_nano_timestamp();
        inner.cleanup(timestamp - self.max_window_range.as_nanos());

        if inner.weight_sum + weight > self.max_weight_limit {
            return Err("exceed max_weight_limit".to_string());
        }

        let end = inner.end;
        inner.data[end] = Elem(timestamp, weight);
        inner.end = (inner.end + 1) % inner.data.len();
        inner.size += 1;
        inner.weight_sum += weight;

        Ok(())
    }

    pub async fn wait(&self, weight: u64) -> Result<(), String> {
        if weight == 0 {
            return Err("weight must be greater than 0".to_string());
        }
        if weight > self.max_weight_limit {
            return Err(format!(
                "weight must be less than or equal to max_weight_limit: {}",
                self.max_weight_limit
            ));
        }

        loop {
            let mut inner = self.inner.lock().await;

            let timestamp = get_current_nano_timestamp();
            inner.cleanup(timestamp - self.max_window_range.as_nanos());

            if inner.weight_sum + weight <= self.max_weight_limit {
                let end = inner.end;
                inner.data[end] = Elem(timestamp, weight);
                inner.end = (inner.end + 1) % inner.data.len();
                inner.size += 1;
                inner.weight_sum += weight;
                return Ok(());
            }

            let sleep_duration = {
                let earliest_timestamp = inner.data[inner.start].0;
                let wait_time = (earliest_timestamp + self.max_window_range.as_nanos())
                    .saturating_sub(timestamp);
                Duration::from_nanos(wait_time as u64)
            };

            drop(inner);

            if sleep_duration.is_zero() {
                continue;
            }
            tokio::time::sleep(sleep_duration).await;
        }
    }
}
