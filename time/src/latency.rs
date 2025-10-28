pub struct LatencyGuard {
    process_name: &'static str,
    start: std::time::Instant,
}

impl LatencyGuard {
    pub fn new(process_name: &'static str) -> Self {
        Self {
            process_name,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for LatencyGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        log::info!(
            "{} took {} us, {} ms",
            self.process_name,
            elapsed.as_micros(),
            elapsed.as_millis()
        );
    }
}
