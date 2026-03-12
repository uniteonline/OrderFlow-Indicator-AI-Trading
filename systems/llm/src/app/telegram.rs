use crate::app::config::TelegramApiConfig;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct TelegramOperator {
    token: String,
    chat_id: String,
    base_api_url: String,
}

#[derive(Debug, Clone)]
pub struct TradeSignalNotification<'a> {
    pub ts_bucket: DateTime<Utc>,
    pub trigger: &'a str,
    pub symbol: &'a str,
    pub model_name: &'a str,
    pub decision: &'a str,
    pub entry_price: Option<f64>,
    pub leverage: Option<f64>,
    pub risk_reward_ratio: Option<f64>,
    pub take_profit: Option<f64>,
    pub stop_loss: Option<f64>,
    pub reason: &'a str,
}

impl TelegramOperator {
    pub fn from_config(cfg: &TelegramApiConfig) -> Option<Self> {
        let token = cfg.resolved_token().trim().to_string();
        let chat_id = normalize_chat_id(cfg.resolved_chat_id().trim());
        if token.is_empty() || chat_id.is_empty() {
            return None;
        }
        let mut base_api_url = cfg.base_api_url.trim().to_string();
        if base_api_url.is_empty() {
            base_api_url = "https://api.telegram.org".to_string();
        }
        base_api_url = base_api_url.trim_end_matches('/').to_string();
        Some(Self {
            token,
            chat_id,
            base_api_url,
        })
    }

    pub async fn send_trade_signal(
        &self,
        http_client: &Client,
        signal: &TradeSignalNotification<'_>,
    ) -> Result<()> {
        let url = format!("{}/bot{}/sendMessage", self.base_api_url, self.token);
        let text = build_trade_signal_message(signal);
        let payload = json!({
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": true,
        });

        let response = http_client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("call telegram sendMessage")?;
        let status = response.status();
        let body = response
            .text()
            .await
            .context("read telegram sendMessage response body")?;
        if !status.is_success() {
            return Err(anyhow!(
                "telegram sendMessage failed status={} body={}",
                status,
                body
            ));
        }
        if let Ok(parsed) = serde_json::from_str::<Value>(&body) {
            if parsed.get("ok").and_then(Value::as_bool) == Some(false) {
                return Err(anyhow!(
                    "telegram sendMessage returned ok=false body={}",
                    body
                ));
            }
        }
        Ok(())
    }
}

fn normalize_chat_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // Numeric channel/group IDs are passed through directly.
    if trimmed.starts_with('-') || trimmed.chars().all(|c| c.is_ascii_digit()) {
        return trimmed.to_string();
    }

    if trimmed.starts_with('@') {
        return trimmed.to_string();
    }

    // Accept links like t.me/<username> and normalize them to @<username>.
    let mut candidate = trimmed
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_start_matches("t.me/")
        .trim_start_matches("telegram.me/");
    candidate = candidate.trim_start_matches('@');
    candidate = candidate.trim_matches('/');
    if let Some((head, _)) = candidate.split_once('?') {
        candidate = head;
    }
    if let Some((head, _)) = candidate.split_once('#') {
        candidate = head;
    }

    if candidate.is_empty() {
        String::new()
    } else {
        format!("@{}", candidate)
    }
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
        "🚨 黄大发 Trade Signal\n{} {}\n📌 Symbol: {}\n🟢 Entry: {}\n⚙️ Leverage: {}\n📊 RR: {}\n🎯 TP: {}\n🛑 SL: {}\n🕒 Time: {}\n🧠 reason: {}",
        direction_emoji,
        signal.decision,
        signal.symbol,
        format_opt_price(signal.entry_price),
        format_opt_leverage(signal.leverage),
        format_opt_ratio(signal.risk_reward_ratio),
        format_opt_price(signal.take_profit),
        format_opt_price(signal.stop_loss),
        format!("{} UTC", time_only_utc),
        signal.reason.replace('\n', " "),
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
