use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    market_data_ingestor::observability::tracing::init();
    let ctx = market_data_ingestor::app::bootstrap::bootstrap().await?;
    market_data_ingestor::app::runtime::run(ctx).await
}
