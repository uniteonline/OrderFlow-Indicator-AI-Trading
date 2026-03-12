use crate::app::config::XApiConfig;
use crate::app::telegram::TradeSignalNotification;
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::Utc;
use hmac::{Hmac, Mac};
use reqwest::{Client, Url};
use serde_json::{json, Value};
use sha1::Sha1;
use std::fmt::Write;
use uuid::Uuid;

type HmacSha1 = Hmac<Sha1>;

#[derive(Debug, Clone)]
pub struct XOperator {
    consumer_key: String,
    secret_key: String,
    access_token: String,
    access_token_secret: String,
    base_api_url: String,
}

impl XOperator {
    pub fn from_config(cfg: &XApiConfig) -> Option<Self> {
        let consumer_key = cfg.resolved_consumer_key().trim().to_string();
        let secret_key = cfg.resolved_secret_key().trim().to_string();
        let access_token = cfg.resolved_access_token().trim().to_string();
        let access_token_secret = cfg.resolved_access_token_secret().trim().to_string();
        if consumer_key.is_empty()
            || secret_key.is_empty()
            || access_token.is_empty()
            || access_token_secret.is_empty()
        {
            return None;
        }

        let mut base_api_url = cfg.base_api_url.trim().to_string();
        if base_api_url.is_empty() {
            base_api_url = "https://api.x.com/2".to_string();
        }
        base_api_url = base_api_url.trim_end_matches('/').to_string();

        Some(Self {
            consumer_key,
            secret_key,
            access_token,
            access_token_secret,
            base_api_url,
        })
    }

    pub async fn send_trade_signal(
        &self,
        http_client: &Client,
        signal: &TradeSignalNotification<'_>,
    ) -> Result<()> {
        let url = format!("{}/tweets", self.base_api_url);
        let nonce = Uuid::new_v4().simple().to_string();
        let timestamp = Utc::now().timestamp().to_string();
        let authorization = self
            .build_oauth1_authorization_header("POST", &url, &nonce, &timestamp)
            .context("build x oauth1 authorization header")?;

        let payload = json!({
            "text": build_trade_signal_message(signal),
        });

        let response = http_client
            .post(&url)
            .header("Authorization", authorization)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("call x create tweet")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("read x create tweet response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "x create tweet failed status={} body={}",
                status,
                body
            ));
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&body) {
            if parsed.get("errors").is_some() {
                return Err(anyhow!("x create tweet returned errors body={}", body));
            }
        }

        Ok(())
    }

    fn build_oauth1_authorization_header(
        &self,
        method: &str,
        request_url: &str,
        nonce: &str,
        timestamp: &str,
    ) -> Result<String> {
        let parsed = Url::parse(request_url)
            .with_context(|| format!("parse request url for oauth1: {}", request_url))?;
        let base_url = oauth_base_url(&parsed)?;

        let mut signature_params = Vec::<(String, String)>::new();
        signature_params.push(("oauth_consumer_key".to_string(), self.consumer_key.clone()));
        signature_params.push(("oauth_nonce".to_string(), nonce.to_string()));
        signature_params.push((
            "oauth_signature_method".to_string(),
            "HMAC-SHA1".to_string(),
        ));
        signature_params.push(("oauth_timestamp".to_string(), timestamp.to_string()));
        signature_params.push(("oauth_token".to_string(), self.access_token.clone()));
        signature_params.push(("oauth_version".to_string(), "1.0".to_string()));
        for (k, v) in parsed.query_pairs() {
            signature_params.push((k.into_owned(), v.into_owned()));
        }

        let signature_base = oauth_signature_base_string(method, &base_url, &signature_params);
        let signing_key = format!(
            "{}&{}",
            oauth_percent_encode(&self.secret_key),
            oauth_percent_encode(&self.access_token_secret)
        );
        let mut mac = HmacSha1::new_from_slice(signing_key.as_bytes())
            .map_err(|err| anyhow!("init oauth1 hmac-sha1 failed: {err}"))?;
        mac.update(signature_base.as_bytes());
        let signature = STANDARD.encode(mac.finalize().into_bytes());

        let mut header_params = vec![
            ("oauth_consumer_key".to_string(), self.consumer_key.clone()),
            ("oauth_nonce".to_string(), nonce.to_string()),
            ("oauth_signature".to_string(), signature),
            (
                "oauth_signature_method".to_string(),
                "HMAC-SHA1".to_string(),
            ),
            ("oauth_timestamp".to_string(), timestamp.to_string()),
            ("oauth_token".to_string(), self.access_token.clone()),
            ("oauth_version".to_string(), "1.0".to_string()),
        ];
        header_params.sort_by(|a, b| a.0.cmp(&b.0));

        let mut output = String::from("OAuth ");
        for (idx, (k, v)) in header_params.iter().enumerate() {
            if idx > 0 {
                output.push_str(", ");
            }
            write!(
                &mut output,
                "{}=\"{}\"",
                oauth_percent_encode(k),
                oauth_percent_encode(v)
            )
            .expect("write to String cannot fail");
        }

        Ok(output)
    }
}

fn oauth_signature_base_string(
    method: &str,
    base_url: &str,
    params: &[(String, String)],
) -> String {
    let mut encoded_params = params
        .iter()
        .map(|(k, v)| (oauth_percent_encode(k), oauth_percent_encode(v)))
        .collect::<Vec<_>>();
    encoded_params.sort();
    let param_string = encoded_params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    format!(
        "{}&{}&{}",
        method.to_ascii_uppercase(),
        oauth_percent_encode(base_url),
        oauth_percent_encode(&param_string)
    )
}

fn oauth_base_url(url: &Url) -> Result<String> {
    let scheme = url.scheme().to_ascii_lowercase();
    let host = url
        .host_str()
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| anyhow!("oauth1 url has no host: {}", url))?;
    let include_port = match (scheme.as_str(), url.port()) {
        ("http", Some(80) | None) => None,
        ("https", Some(443) | None) => None,
        (_, port) => port,
    };

    let mut base = format!("{scheme}://{host}");
    if let Some(port) = include_port {
        write!(&mut base, ":{port}").expect("write to String cannot fail");
    }
    base.push_str(url.path());
    Ok(base)
}

fn oauth_percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            write!(&mut out, "%{b:02X}").expect("write to String cannot fail");
        }
    }
    out
}

fn build_trade_signal_message(signal: &TradeSignalNotification<'_>) -> String {
    let direction_emoji = if signal.decision.eq_ignore_ascii_case("LONG") {
        "📈"
    } else if signal.decision.eq_ignore_ascii_case("SHORT") {
        "📉"
    } else {
        "ℹ️"
    };
    let time_only_utc = signal.ts_bucket.format("%H:%M:%S").to_string();
    format!(
        "{} {}\n📌 Symbol: {}\n🟢 Entry: {}\n⚙️ Leverage: {}\n📊 RR: {}\n🎯 TP: {}\n🛑 SL: {}\n🕒 Time: {}",
        direction_emoji,
        signal.decision,
        signal.symbol,
        format_opt_price(signal.entry_price),
        format_opt_leverage(signal.leverage),
        format_opt_ratio(signal.risk_reward_ratio),
        format_opt_price(signal.take_profit),
        format_opt_price(signal.stop_loss),
        format!("{} UTC", time_only_utc),
    )
}

fn format_opt_price(value: Option<f64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn format_opt_ratio(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "-".to_string())
}

fn format_opt_leverage(value: Option<f64>) -> String {
    value
        .map(|v| {
            if (v - v.round()).abs() < f64::EPSILON {
                format!("{}", v.round() as i64)
            } else {
                format!("{:.2}", v)
            }
        })
        .unwrap_or_else(|| "-".to_string())
}
