use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    indicator_engine::observability::tracing::init();
    let ctx = indicator_engine::app::bootstrap::bootstrap().await?;
    indicator_engine::app::runtime::run(ctx).await
}
