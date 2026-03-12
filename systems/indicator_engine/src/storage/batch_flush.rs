#[derive(Debug, Clone, Copy)]
pub struct FlushPolicy {
    pub max_rows: usize,
    pub max_millis: u64,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            max_rows: 1000,
            max_millis: 200,
        }
    }
}
