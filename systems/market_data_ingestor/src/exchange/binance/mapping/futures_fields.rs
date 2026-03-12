use serde_json::Value;

pub fn symbol_from_stream(stream: &str) -> Option<String> {
    stream.split('@').next().map(|s| s.to_uppercase())
}

pub fn event_ts_millis(value: &Value) -> Option<i64> {
    value
        .get("E")
        .and_then(Value::as_i64)
        .or_else(|| value.get("T").and_then(Value::as_i64))
}
