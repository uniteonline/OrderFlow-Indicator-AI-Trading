use crate::indicators::context::IndicatorContext;

pub fn spot_flow_confirms_futures(ctx: &IndicatorContext, same_direction: bool) -> bool {
    if same_direction {
        ctx.spot.delta.signum() == ctx.futures.delta.signum() || ctx.spot.delta.abs() < 1e-9
    } else {
        ctx.spot.delta.signum() != ctx.futures.delta.signum()
    }
}
