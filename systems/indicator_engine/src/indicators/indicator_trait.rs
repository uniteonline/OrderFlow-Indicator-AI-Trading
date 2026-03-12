use crate::indicators::context::{IndicatorComputation, IndicatorContext};

pub trait Indicator: Send + Sync {
    fn code(&self) -> &'static str;
    fn evaluate(&self, ctx: &IndicatorContext) -> IndicatorComputation;
}
