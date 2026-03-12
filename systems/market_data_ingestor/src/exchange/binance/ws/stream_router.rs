#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Trade,
    Depth,
    Bbo,
    Kline,
    MarkPrice,
    ForceOrder,
    Unknown,
}

pub fn classify_stream(stream: &str) -> StreamKind {
    if stream.ends_with("@aggTrade") {
        return StreamKind::Trade;
    }
    if stream.contains("@depth") {
        return StreamKind::Depth;
    }
    if stream.ends_with("@bookTicker") {
        return StreamKind::Bbo;
    }
    if stream.contains("@kline_") {
        return StreamKind::Kline;
    }
    if stream.contains("@markPrice") {
        return StreamKind::MarkPrice;
    }
    if stream.ends_with("@forceOrder") {
        return StreamKind::ForceOrder;
    }
    StreamKind::Unknown
}
