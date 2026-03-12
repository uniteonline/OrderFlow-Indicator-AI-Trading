mod app;
mod execution;
mod llm;
mod observability;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    observability::tracing::init();
    let ctx = app::bootstrap::bootstrap().await?;
    app::runtime::run(ctx).await
}
