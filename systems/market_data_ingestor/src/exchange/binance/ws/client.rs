use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    client_async_tls_with_config, connect_async, tungstenite::client::IntoClientRequest,
    tungstenite::http::Request, tungstenite::http::Uri, tungstenite::protocol::Message,
    MaybeTlsStream, WebSocketStream,
};

#[derive(Debug, Deserialize)]
pub struct CombinedStreamEvent {
    pub stream: String,
    pub data: Value,
}

pub fn combined_stream_url(base_url: &str, streams: &[String]) -> String {
    format!(
        "{}/stream?streams={}",
        base_url.trim_end_matches('/'),
        streams.join("/")
    )
}

pub async fn connect_combined_stream(
    base_url: &str,
    streams: &[String],
    proxy_url: Option<&str>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let url = combined_stream_url(base_url, streams);
    let request = url.into_client_request()?;

    if let Some(proxy) = proxy_url {
        connect_via_http_proxy(request, proxy).await
    } else {
        let (ws_stream, _) = connect_async(request).await?;
        Ok(ws_stream)
    }
}

pub fn decode_text_message(text: &str) -> Result<CombinedStreamEvent> {
    Ok(serde_json::from_str(text)?)
}

pub fn is_terminal_message(msg: &Message) -> bool {
    matches!(msg, Message::Close(_))
}

async fn connect_via_http_proxy(
    request: Request<()>,
    proxy_url: &str,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let target_uri = request.uri().clone();
    let (target_host, target_port) = parse_target_endpoint(&target_uri)?;
    let (proxy_host, proxy_port) = parse_proxy_endpoint(proxy_url)?;

    let mut socket = TcpStream::connect((proxy_host.as_str(), proxy_port))
        .await
        .with_context(|| format!("connect websocket proxy {}:{}", proxy_host, proxy_port))?;
    establish_connect_tunnel(&mut socket, &target_host, target_port).await?;

    let (ws_stream, _) = client_async_tls_with_config(request, socket, None, None).await?;
    Ok(ws_stream)
}

fn parse_target_endpoint(uri: &Uri) -> Result<(String, u16)> {
    let host = uri
        .host()
        .ok_or_else(|| anyhow!("websocket target URI has no host"))?
        .to_string();
    let scheme = uri.scheme_str().unwrap_or("wss");
    let port = uri
        .port_u16()
        .unwrap_or(if scheme.eq_ignore_ascii_case("wss") {
            443
        } else {
            80
        });
    Ok((host, port))
}

fn parse_proxy_endpoint(proxy_url: &str) -> Result<(String, u16)> {
    let uri: Uri = proxy_url
        .parse()
        .with_context(|| format!("parse proxy url: {}", proxy_url))?;
    let scheme = uri.scheme_str().unwrap_or("http");
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
        return Err(anyhow!(
            "unsupported websocket proxy scheme {}; expected http/https",
            scheme
        ));
    }
    let host = uri
        .host()
        .ok_or_else(|| anyhow!("proxy URI has no host: {}", proxy_url))?
        .to_string();
    let port = uri
        .port_u16()
        .unwrap_or(if scheme.eq_ignore_ascii_case("https") {
            443
        } else {
            80
        });
    Ok((host, port))
}

async fn establish_connect_tunnel(
    socket: &mut TcpStream,
    target_host: &str,
    target_port: u16,
) -> Result<()> {
    let req = format!(
        "CONNECT {0}:{1} HTTP/1.1\r\nHost: {0}:{1}\r\nProxy-Connection: Keep-Alive\r\n\r\n",
        target_host, target_port
    );
    socket
        .write_all(req.as_bytes())
        .await
        .context("write HTTP CONNECT request to proxy")?;

    let mut response = Vec::with_capacity(1024);
    let mut buf = [0_u8; 512];
    loop {
        let n = socket
            .read(&mut buf)
            .await
            .context("read HTTP CONNECT response from proxy")?;
        if n == 0 {
            return Err(anyhow!(
                "proxy closed connection before completing CONNECT handshake"
            ));
        }
        response.extend_from_slice(&buf[..n]);
        if response.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if response.len() > 8192 {
            return Err(anyhow!("proxy CONNECT response header too large"));
        }
    }

    let response_text = String::from_utf8_lossy(&response);
    let status_line = response_text.lines().next().unwrap_or_default();
    if !(status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200")) {
        return Err(anyhow!("proxy CONNECT failed: {}", status_line));
    }

    Ok(())
}
