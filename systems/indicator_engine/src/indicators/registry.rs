use crate::indicators::i01_price_volume_structure::I01PriceVolumeStructure;
use crate::indicators::i02_footprint::I02Footprint;
use crate::indicators::i03_divergence::I03Divergence;
use crate::indicators::i04_liquidation_density::I04LiquidationDensity;
use crate::indicators::i05_orderbook_depth::I05OrderbookDepth;
use crate::indicators::i06_absorption::I06Absorption;
use crate::indicators::i07_initiation::I07Initiation;
use crate::indicators::i08_bullish_absorption::I08BullishAbsorption;
use crate::indicators::i09_bullish_initiation::I09BullishInitiation;
use crate::indicators::i10_bearish_absorption::I10BearishAbsorption;
use crate::indicators::i11_bearish_initiation::I11BearishInitiation;
use crate::indicators::i12_buying_exhaustion::I12BuyingExhaustion;
use crate::indicators::i13_selling_exhaustion::I13SellingExhaustion;
use crate::indicators::i14_cvd_pack::I14CvdPack;
use crate::indicators::i15_whale_trades::I15WhaleTrades;
use crate::indicators::i16_funding_rate::I16FundingRate;
use crate::indicators::i17_vpin::I17Vpin;
use crate::indicators::i18_avwap::I18Avwap;
use crate::indicators::i19_kline_history::I19KlineHistory;
use crate::indicators::i20_tpo_market_profile::I20TpoMarketProfile;
use crate::indicators::i21_rvwap_sigma_bands::I21RvwapSigmaBands;
use crate::indicators::i22_high_volume_pulse::I22HighVolumePulse;
use crate::indicators::i23_ema_trend_regime::I23EmaTrendRegime;
use crate::indicators::i24_fvg::I24Fvg;
use crate::indicators::indicator_trait::Indicator;
use std::sync::Arc;

pub fn build_registry() -> Vec<Arc<dyn Indicator>> {
    vec![
        Arc::new(I01PriceVolumeStructure),
        Arc::new(I02Footprint),
        Arc::new(I03Divergence),
        Arc::new(I04LiquidationDensity),
        Arc::new(I05OrderbookDepth),
        Arc::new(I06Absorption),
        Arc::new(I07Initiation),
        Arc::new(I08BullishAbsorption),
        Arc::new(I09BullishInitiation),
        Arc::new(I10BearishAbsorption),
        Arc::new(I11BearishInitiation),
        Arc::new(I12BuyingExhaustion),
        Arc::new(I13SellingExhaustion),
        Arc::new(I14CvdPack),
        Arc::new(I15WhaleTrades),
        Arc::new(I16FundingRate),
        Arc::new(I17Vpin),
        Arc::new(I18Avwap),
        Arc::new(I19KlineHistory),
        Arc::new(I20TpoMarketProfile),
        Arc::new(I21RvwapSigmaBands),
        Arc::new(I22HighVolumePulse),
        Arc::new(I23EmaTrendRegime),
        Arc::new(I24Fvg),
    ]
}
