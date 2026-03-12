use crate::ingest::decoder::{EngineEvent, MdData};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamLane {
    Trade,
    Orderbook,
    Kline,
    MarkFunding,
    ForceOrder,
}

pub fn classify(event: &EngineEvent) -> StreamLane {
    match event.data {
        MdData::Trade(_) => StreamLane::Trade,
        MdData::AggTrade1m(_) => StreamLane::Trade,
        MdData::Depth(_) | MdData::OrderbookSnapshot(_) | MdData::Bbo(_) => StreamLane::Orderbook,
        MdData::AggOrderbook1m(_) => StreamLane::Orderbook,
        MdData::Kline(_) => StreamLane::Kline,
        MdData::MarkPrice(_) | MdData::FundingRate(_) => StreamLane::MarkFunding,
        MdData::AggFundingMark1m(_) => StreamLane::MarkFunding,
        MdData::ForceOrder(_) | MdData::AggLiq1m(_) => StreamLane::ForceOrder,
    }
}
