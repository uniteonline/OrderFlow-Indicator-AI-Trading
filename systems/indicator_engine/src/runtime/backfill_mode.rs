#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillMode {
    Disabled,
    Enabled,
}

impl BackfillMode {
    pub fn as_bool(self) -> bool {
        matches!(self, Self::Enabled)
    }
}
