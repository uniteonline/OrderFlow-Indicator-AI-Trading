use crate::exchange::binance::rest::agg_trades::BinanceAggTrade;
use crate::exchange::binance::rest::depth_snapshot::BinanceDepthSnapshot;
use crate::exchange::binance::rest::exchange_info::BinanceExchangeInfo;
use crate::exchange::binance::rest::funding_rate::BinanceFundingRateRecord;
use crate::exchange::binance::rest::klines::BinanceKlineRow;
use crate::exchange::binance::rest::premium_index::BinancePremiumIndex;
use anyhow::{anyhow, Context, Result};
use reqwest::{header::CONTENT_TYPE, Client, StatusCode};
use serde_json::Value;
use std::error::Error as StdError;
use std::time::Duration;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct RestRetryPolicy {
    pub enabled: bool,
    pub max_retries: u32,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for RestRetryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 3,
            base_backoff_ms: 500,
            max_backoff_ms: 8_000,
        }
    }
}

#[derive(Clone)]
pub struct BinanceRestClient {
    client: Client,
    retry: RestRetryPolicy,
}

impl BinanceRestClient {
    pub fn new(client: Client, retry: RestRetryPolicy) -> Self {
        Self { client, retry }
    }

    pub async fn fetch_depth_snapshot(
        &self,
        market: &str,
        symbol: &str,
        limit: u16,
    ) -> Result<BinanceDepthSnapshot> {
        let url = format!("{}{}", base_url(market), depth_path(market));
        let symbol = symbol.to_string();
        let limit_str = limit.to_string();

        let response = self
            .send_with_retry("request depth snapshot", &url, || {
                self.client
                    .get(&url)
                    .query(&[("symbol", symbol.as_str()), ("limit", limit_str.as_str())])
            })
            .await
            .context("request depth snapshot")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<read body failed>".to_string());
            return Err(anyhow!(
                "depth snapshot bad status status={} market={} symbol={} limit={} body={}",
                status,
                market,
                symbol,
                limit,
                shorten_body(&body, 300)
            ));
        }

        let snapshot = response
            .json::<BinanceDepthSnapshot>()
            .await
            .context("decode depth snapshot")?;

        Ok(snapshot)
    }

    pub async fn fetch_exchange_info(
        &self,
        market: &str,
        symbol: Option<&str>,
    ) -> Result<BinanceExchangeInfo> {
        let url = format!("{}{}", base_url(market), exchange_info_path(market));
        let symbol = symbol.map(ToString::to_string);

        self.send_with_retry("request exchange info", &url, || {
            let mut request = self.client.get(&url);
            if let Some(sym) = symbol.as_deref() {
                request = request.query(&[("symbol", sym)]);
            }
            request
        })
        .await
        .context("request exchange info")?
        .error_for_status()
        .context("exchange info bad status")?
        .json::<BinanceExchangeInfo>()
        .await
        .context("decode exchange info")
    }

    pub async fn fetch_agg_trades(
        &self,
        market: &str,
        symbol: &str,
        from_id: Option<i64>,
        limit: u16,
    ) -> Result<Vec<BinanceAggTrade>> {
        let url = format!("{}{}", base_url(market), agg_trades_path(market));
        let mut query: Vec<(&str, String)> =
            vec![("symbol", symbol.to_string()), ("limit", limit.to_string())];
        if let Some(id) = from_id {
            query.push(("fromId", id.to_string()));
        }

        let response = self
            .send_with_retry("request agg trades", &url, || {
                self.client.get(&url).query(&query)
            })
            .await
            .context("request agg trades")?;

        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let body = response.text().await.context("read agg trades body")?;

        if !status.is_success() {
            warn!(
                market = market,
                symbol = symbol,
                from_id = ?from_id,
                limit = limit,
                status = %status,
                content_type = %content_type,
                url = %url,
                body = %shorten_body(&body, 500),
                "agg trades request returned non-success status"
            );
            return Err(anyhow!(
                "agg trades bad status status={} market={} symbol={} from_id={:?} limit={} content_type={} body={}",
                status,
                market,
                symbol,
                from_id,
                limit,
                content_type,
                shorten_body(&body, 300)
            ));
        }

        let rows = serde_json::from_str::<Vec<BinanceAggTrade>>(&body).context("decode agg trades");

        let rows = match rows {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    market = market,
                    symbol = symbol,
                    from_id = ?from_id,
                    limit = limit,
                    status = %status,
                    content_type = %content_type,
                    url = %url,
                    body = %shorten_body(&body, 500),
                    error = %err,
                    "agg trades decode failed with unexpected response payload"
                );
                return Err(err);
            }
        };

        Ok(rows)
    }

    pub async fn fetch_klines(
        &self,
        market: &str,
        symbol: &str,
        interval: &str,
        start_time_ms: Option<i64>,
        end_time_ms: Option<i64>,
        limit: u16,
    ) -> Result<Vec<BinanceKlineRow>> {
        let url = format!("{}{}", base_url(market), klines_path(market));
        let mut query: Vec<(&str, String)> = vec![
            ("symbol", symbol.to_string()),
            ("interval", interval.to_string()),
            ("limit", limit.to_string()),
        ];

        if let Some(start) = start_time_ms {
            query.push(("startTime", start.to_string()));
        }
        if let Some(end) = end_time_ms {
            query.push(("endTime", end.to_string()));
        }

        let raw_rows = self
            .send_with_retry("request klines", &url, || {
                self.client.get(&url).query(&query)
            })
            .await
            .context("request klines")?
            .error_for_status()
            .context("klines bad status")?
            .json::<Vec<Vec<Value>>>()
            .await
            .context("decode klines")?;

        raw_rows
            .iter()
            .map(|row| BinanceKlineRow::from_row(row))
            .collect()
    }

    pub async fn fetch_premium_index(&self, symbol: &str) -> Result<BinancePremiumIndex> {
        let url = "https://fapi.binance.com/fapi/v1/premiumIndex";
        let response = self
            .send_with_retry("request premium index", url, || {
                self.client.get(url).query(&[("symbol", symbol)])
            })
            .await
            .map_err(|err| {
                let class = classify_reqwest_error(&err);
                let chain = error_chain_string(&err);
                warn!(
                    symbol = symbol,
                    url = url,
                    error_category = class.category,
                    failure_dns = class.is_dns,
                    failure_tls = class.is_tls,
                    failure_proxy_handshake = class.is_proxy_handshake,
                    failure_connect = class.is_connect,
                    failure_timeout = class.is_timeout,
                    failure_http_5xx = class.is_http_5xx,
                    failure_http_429 = class.is_http_429,
                    is_timeout = err.is_timeout(),
                    is_connect = err.is_connect(),
                    is_request = err.is_request(),
                    is_body = err.is_body(),
                    is_decode = err.is_decode(),
                    status = ?err.status(),
                    error_chain = %chain,
                    error = %err,
                    "premium index request failed before response"
                );
                anyhow!(
                    "request premium index symbol={} url={} category={} timeout={} connect={} status={:?} err={}",
                    symbol,
                    url,
                    class.category,
                    err.is_timeout(),
                    err.is_connect(),
                    err.status(),
                    err
                )
            })?;

        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        let body = response.text().await.context("read premium index body")?;

        if !status.is_success() {
            warn!(
                symbol = symbol,
                status = %status,
                content_type = %content_type,
                url = url,
                body = %shorten_body(&body, 500),
                "premium index request returned non-success status"
            );
            return Err(anyhow!(
                "premium index bad status status={} symbol={} content_type={} body={}",
                status,
                symbol,
                content_type,
                shorten_body(&body, 300)
            ));
        }

        let premium =
            serde_json::from_str::<BinancePremiumIndex>(&body).context("decode premium index");
        let premium = match premium {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    symbol = symbol,
                    status = %status,
                    content_type = %content_type,
                    url = url,
                    body = %shorten_body(&body, 500),
                    error = %err,
                    "premium index decode failed with unexpected response payload"
                );
                return Err(err);
            }
        };

        Ok(premium)
    }

    pub async fn fetch_latest_funding_rate(
        &self,
        symbol: &str,
    ) -> Result<Option<BinanceFundingRateRecord>> {
        let url = "https://fapi.binance.com/fapi/v1/fundingRate";

        let mut rows = self
            .send_with_retry("request funding rate", url, || {
                self.client
                    .get(url)
                    .query(&[("symbol", symbol), ("limit", "1")])
            })
            .await
            .context("request funding rate")?
            .error_for_status()
            .context("funding rate bad status")?
            .json::<Vec<BinanceFundingRateRecord>>()
            .await
            .context("decode funding rate")?;

        Ok(rows.pop())
    }

    async fn send_with_retry<F>(
        &self,
        op_name: &str,
        url: &str,
        build_request: F,
    ) -> std::result::Result<reqwest::Response, reqwest::Error>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let max_retries = if self.retry.enabled {
            self.retry.max_retries
        } else {
            0
        };
        let total_attempts = max_retries.saturating_add(1);
        let mut attempt: u32 = 0;

        loop {
            let response = build_request().send().await;
            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if !is_retryable_status(status) || attempt >= max_retries {
                        return Ok(resp);
                    }

                    let class = classify_status(status);
                    let backoff = self.retry_backoff(attempt);
                    warn!(
                        op = op_name,
                        url = url,
                        attempt = attempt + 1,
                        max_attempts = total_attempts,
                        error_category = class.category,
                        failure_http_5xx = class.is_http_5xx,
                        failure_http_429 = class.is_http_429,
                        status = %status,
                        backoff_ms = backoff.as_millis() as u64,
                        "rest request got retryable status, retrying"
                    );
                    tokio::time::sleep(backoff).await;
                }
                Err(err) => {
                    if !is_retryable_error(&err) || attempt >= max_retries {
                        return Err(err);
                    }

                    let class = classify_reqwest_error(&err);
                    let chain = error_chain_string(&err);
                    let backoff = self.retry_backoff(attempt);
                    warn!(
                        op = op_name,
                        url = url,
                        attempt = attempt + 1,
                        max_attempts = total_attempts,
                        error_category = class.category,
                        failure_dns = class.is_dns,
                        failure_tls = class.is_tls,
                        failure_proxy_handshake = class.is_proxy_handshake,
                        failure_connect = class.is_connect,
                        failure_timeout = class.is_timeout,
                        failure_http_5xx = class.is_http_5xx,
                        failure_http_429 = class.is_http_429,
                        is_timeout = err.is_timeout(),
                        is_connect = err.is_connect(),
                        is_request = err.is_request(),
                        status = ?err.status(),
                        error_chain = %chain,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %err,
                        "rest request failed, retrying with exponential backoff"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }

            attempt = attempt.saturating_add(1);
        }
    }

    fn retry_backoff(&self, retry_index: u32) -> Duration {
        let exp = retry_index.min(16);
        let factor = 1_u64 << exp;
        let backoff = self.retry.base_backoff_ms.saturating_mul(factor);
        let capped = backoff.min(self.retry.max_backoff_ms.max(self.retry.base_backoff_ms));
        Duration::from_millis(capped.max(1))
    }
}

fn shorten_body(body: &str, max_len: usize) -> String {
    if body.len() <= max_len {
        body.to_string()
    } else {
        let mut out = body[..max_len].to_string();
        out.push_str("...");
        out
    }
}

fn base_url(market: &str) -> &'static str {
    match market {
        "spot" => "https://api.binance.com",
        "futures" => "https://fapi.binance.com",
        _ => "https://fapi.binance.com",
    }
}

fn depth_path(market: &str) -> &'static str {
    match market {
        "spot" => "/api/v3/depth",
        "futures" => "/fapi/v1/depth",
        _ => "/fapi/v1/depth",
    }
}

fn agg_trades_path(market: &str) -> &'static str {
    match market {
        "spot" => "/api/v3/aggTrades",
        "futures" => "/fapi/v1/aggTrades",
        _ => "/fapi/v1/aggTrades",
    }
}

fn klines_path(market: &str) -> &'static str {
    match market {
        "spot" => "/api/v3/klines",
        "futures" => "/fapi/v1/klines",
        _ => "/fapi/v1/klines",
    }
}

fn exchange_info_path(market: &str) -> &'static str {
    match market {
        "spot" => "/api/v3/exchangeInfo",
        "futures" => "/fapi/v1/exchangeInfo",
        _ => "/fapi/v1/exchangeInfo",
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 425 | 429)
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    if err.is_timeout() || err.is_connect() || err.is_request() {
        return true;
    }
    err.status().map(is_retryable_status).unwrap_or(false)
}

#[derive(Debug, Clone, Copy)]
struct RestFailureClass {
    category: &'static str,
    is_dns: bool,
    is_tls: bool,
    is_proxy_handshake: bool,
    is_connect: bool,
    is_timeout: bool,
    is_http_5xx: bool,
    is_http_429: bool,
}

fn classify_status(status: StatusCode) -> RestFailureClass {
    let is_http_5xx = status.is_server_error();
    let is_http_429 = status == StatusCode::TOO_MANY_REQUESTS;
    let category = if is_http_429 {
        "http_429"
    } else if is_http_5xx {
        "http_5xx"
    } else {
        "http_retryable_other"
    };

    RestFailureClass {
        category,
        is_dns: false,
        is_tls: false,
        is_proxy_handshake: false,
        is_connect: false,
        is_timeout: false,
        is_http_5xx,
        is_http_429,
    }
}

fn classify_reqwest_error(err: &reqwest::Error) -> RestFailureClass {
    let chain = error_chain_string(err).to_lowercase();
    let text = format!("{} {}", err, chain).to_lowercase();

    let is_timeout = err.is_timeout();
    let is_dns = contains_any(
        &text,
        &[
            "dns error",
            "failed to lookup address information",
            "no such host is known",
            "failed to lookup host",
            "name or service not known",
            "nodename nor servname provided",
        ],
    );
    let is_proxy_handshake = contains_any(
        &text,
        &[
            "proxy connect",
            "proxyconnect",
            "tunnel",
            "connect proxy",
            "proxy authentication",
            "proxy error",
            "socks",
        ],
    );
    let is_tls = contains_any(
        &text,
        &[
            "tls",
            "ssl",
            "certificate",
            "rustls",
            "handshake",
            "webpki",
            "pkix",
        ],
    );
    let is_connect = err.is_connect()
        || contains_any(
            &text,
            &[
                "connection reset",
                "connection refused",
                "connection aborted",
                "failed to connect",
                "network is unreachable",
                "host unreachable",
                "os error 10053",
                "os error 10054",
                "os error 10060",
            ],
        );
    let status = err.status();
    let is_http_5xx = status.map(|s| s.is_server_error()).unwrap_or(false);
    let is_http_429 = status == Some(StatusCode::TOO_MANY_REQUESTS);

    let category = if is_timeout {
        "timeout"
    } else if is_dns {
        "dns"
    } else if is_proxy_handshake {
        "proxy_handshake"
    } else if is_tls {
        "tls"
    } else if is_http_429 {
        "http_429"
    } else if is_http_5xx {
        "http_5xx"
    } else if is_connect {
        "connect"
    } else if err.is_request() {
        "request_transport"
    } else if err.is_body() {
        "body_read"
    } else if err.is_decode() {
        "decode"
    } else {
        "unknown"
    };

    RestFailureClass {
        category,
        is_dns,
        is_tls,
        is_proxy_handshake,
        is_connect,
        is_timeout,
        is_http_5xx,
        is_http_429,
    }
}

fn error_chain_string(err: &reqwest::Error) -> String {
    let mut chain = Vec::new();
    let mut cur = err.source();
    while let Some(src) = cur {
        chain.push(src.to_string());
        cur = src.source();
        if chain.len() >= 8 {
            break;
        }
    }
    chain.join(" | ")
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}
