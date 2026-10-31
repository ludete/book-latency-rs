use crate::venue::Venue;
use anyhow::{Context, Result, bail, ensure};
use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    io::{self, Write},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout, timeout_at};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Error as WsError, Message, protocol::WebSocketConfig},
};

pub struct Probe {
    pub venue: Venue,
    pub symbol: String,
    pub market: Option<String>,
    pub samples: Vec<f64>,
    pub invalid: usize,
    pub error: Option<String>,
}

fn unix_time() -> Result<Duration> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("host clock is before Unix epoch")
}

fn timestamp(value: &Value, divisor: f64) -> Result<f64> {
    let value = value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .context("missing or invalid book timestamp")?;
    Ok(value / divisor)
}

fn subscription_failed(msg: &Value) -> bool {
    let error = &msg["error"];
    let has_error = match error {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    };
    has_error
        || msg["success"] == false
        || matches!(
            msg["type"].as_str(),
            Some("error" | "degraded" | "unsubscribed")
        )
        || msg["channel"] == "error"
        || msg["event"] == "error"
}

impl Probe {
    pub fn new(venue: Venue, symbol: String) -> Self {
        Self {
            venue,
            symbol,
            market: None,
            samples: Vec::new(),
            invalid: 0,
            error: None,
        }
    }

    pub async fn prepare(&mut self, client: &reqwest::Client) {
        match timeout(
            Duration::from_secs(15),
            self.venue.load_market(client, &self.symbol),
        )
        .await
        {
            Ok(Ok(market)) => self.market = Some(market),
            Ok(Err(error)) => self.error = Some(format!("metadata: {error:#}")),
            Err(_) => self.error = Some("metadata timed out after 15s".to_owned()),
        }
    }

    pub async fn run(&mut self, deadline: Instant) {
        self.run_at(self.venue.ws_url(), deadline).await;
    }

    async fn run_at(&mut self, url: &str, deadline: Instant) {
        if let Ok(Err(error)) = timeout_at(deadline, self.collect(url)).await {
            self.error = Some(format!("{error:#}"));
        }
    }

    async fn collect(&mut self, url: &str) -> Result<()> {
        let config = WebSocketConfig {
            max_message_size: Some(1 << 23),
            max_frame_size: Some(1 << 23),
            ..Default::default()
        };
        let connection = timeout(
            Duration::from_secs(10),
            connect_async_with_config(url, Some(config), true),
        )
        .await
        .context("WebSocket connect timed out after 10s")?;
        let (mut ws, _) = match connection {
            Err(WsError::Http(response)) => {
                let body = String::from_utf8_lossy(response.body().as_deref().unwrap_or_default())
                    .chars()
                    .take(512)
                    .collect::<String>();
                bail!(
                    "WebSocket handshake failed: {}: {}",
                    response.status(),
                    body.split_whitespace().collect::<Vec<_>>().join(" ")
                );
            }
            other => other?,
        };
        let subscription = self.venue.subscription(
            self.market.as_deref().context("market not prepared")?,
            unix_time()?.as_secs(),
        );
        if !self.venue.is_lighter() {
            ws.send(Message::Text(subscription.to_string())).await?;
        }
        let mut app_ping = interval_at(
            Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        let mut ws_ping = interval_at(
            Instant::now() + Duration::from_secs(15),
            Duration::from_secs(15),
        );
        app_ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ws_ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut pending_pong = None;
        let mut pong_deadline = Instant::now();
        let mut ping_id = 0u64;
        loop {
            tokio::select! {
                incoming = ws.next() => {
                    // Capture receipt before JSON decoding or book processing.
                    let received_at = unix_time()?.as_secs_f64();
                    let raw = match incoming.context("WebSocket closed before measurement ended")?? {
                        Message::Text(text) => text.into_bytes(),
                        Message::Binary(bytes) => bytes,
                        Message::Ping(_) => { ws.flush().await?; continue; },
                        Message::Pong(payload) => {
                            if pending_pong.as_ref() == Some(&payload) { pending_pong = None; }
                            continue;
                        }
                        Message::Close(frame) => bail!("WebSocket closed before measurement ended: {frame:?}"),
                        Message::Frame(_) => continue,
                    };
                    let msg: Value = serde_json::from_slice(&raw).context("invalid WebSocket JSON")?;
                    ensure!(msg.is_object(), "WebSocket JSON must be an object");
                    ensure!(!subscription_failed(&msg), "subscription failed: {msg}");
                    if self.venue.is_lighter() {
                        if msg["type"] == "connected" {
                            ws.send(Message::Text(subscription.to_string())).await?;
                        } else if msg["type"] == "ping" {
                            ws.send(Message::Text(json!({"type": "pong"}).to_string())).await?;
                        }
                    }
                    self.observe(&msg, received_at);
                }
                _ = app_ping.tick(), if !self.venue.is_lighter() => {
                    if let Some(ping) = self.venue.heartbeat(unix_time()?.as_secs()) {
                        ws.send(Message::Text(ping.to_string())).await?;
                    }
                }
                _ = ws_ping.tick(), if pending_pong.is_none() => {
                    ping_id = ping_id.wrapping_add(1);
                    let payload = ping_id.to_be_bytes().to_vec();
                    ws.send(Message::Ping(payload.clone())).await?;
                    pending_pong = Some(payload);
                    pong_deadline = Instant::now() + Duration::from_secs(15);
                }
                _ = sleep_until(pong_deadline), if pending_pong.is_some() => {
                    bail!("WebSocket pong timed out after 15s");
                }
            }
        }
    }

    fn observe(&mut self, msg: &Value, received_at: f64) {
        let result = self.observe_book(msg, received_at);
        if result.is_err() {
            self.invalid += 1;
        }
    }

    fn observe_book(&mut self, msg: &Value, received_at: f64) -> Result<()> {
        let market = self.market.as_deref().context("market not prepared")?;
        let mut record = |stamp: f64| -> Result<()> {
            ensure!(stamp.is_finite() && stamp > 0.0, "invalid book timestamp");
            let age = (received_at - stamp) * 1000.0;
            ensure!(age.is_finite(), "book age overflow");
            // Negative ages and repeated timestamps must remain visible.
            self.samples.push(age);
            Ok(())
        };
        match self.venue {
            Venue::Hyperliquid | Venue::Entropy | Venue::Tradexyz => {
                if msg["channel"] == "l2Book" {
                    let data = msg["data"].as_object().context("missing book data")?;
                    if data.get("coin").and_then(Value::as_str) == Some(market) {
                        record(timestamp(&msg["data"]["time"], 1000.0)?)?;
                    }
                }
            }
            Venue::Lighter | Venue::LighterRh => {
                if matches!(
                    msg["type"].as_str(),
                    Some("subscribed/order_book" | "update/order_book")
                ) {
                    let id = msg["channel"]
                        .as_str()
                        .and_then(|s| s.rsplit([':', '/']).next())
                        .and_then(|s| s.parse::<u64>().ok());
                    if id.is_some() && id == market.parse().ok() {
                        record(timestamp(
                            &msg["order_book"]["last_updated_at"],
                            1_000_000.0,
                        )?)?;
                    }
                }
            }
            Venue::Gate => {
                if msg["channel"] == "futures.order_book"
                    && matches!(msg["event"].as_str(), Some("all" | "update"))
                {
                    let data = msg["result"].as_object().context("missing book result")?;
                    if data.get("contract").and_then(Value::as_str) == Some(market) {
                        record(timestamp(&msg["result"]["t"], 1000.0)?)?;
                    }
                }
            }
            Venue::Arcus => {
                if msg["channel"] == "l2Orderbook"
                    && msg["id"] == market
                    && matches!(msg["type"].as_str(), Some("subscribed" | "channel_data"))
                {
                    record(timestamp(&msg["contents"]["timestamp"], 1_000_000.0)?)?;
                }
            }
            Venue::Ondo => {
                if msg["channel"] == "depthBooksPerps" && msg["type"] == "update" {
                    for data in msg["data"].as_array().context("missing book data")? {
                        ensure!(data.is_object(), "invalid book data");
                        if data["market"] == market {
                            let stamp = DateTime::parse_from_rfc3339(
                                data["time"].as_str().context("missing Ondo timestamp")?,
                            )?;
                            record(
                                stamp.timestamp() as f64
                                    + stamp.timestamp_subsec_nanos() as f64 / 1e9,
                            )?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let index = (sorted.len() - 1) as f64 * p / 100.0;
    let lo = index.floor() as usize;
    let hi = index.ceil() as usize;
    sorted[lo] + (sorted[hi] - sorted[lo]) * (index - lo as f64)
}

pub fn print_results(probes: &mut [Probe], percentiles: &[f64]) -> io::Result<bool> {
    write_results(&mut io::stdout().lock(), probes, percentiles)
}

fn write_results(
    out: &mut impl Write,
    probes: &mut [Probe],
    percentiles: &[f64],
) -> io::Result<bool> {
    writeln!(
        out,
        "\nBook age at receipt (ms) = local receive time - exchange book time"
    )?;
    writeln!(
        out,
        "Samples include initial snapshots and repeated timestamps; heartbeats excluded."
    )?;
    writeln!(
        out,
        "Negative ages are retained; synchronize the host clock. Percentiles use linear interpolation."
    )?;
    write!(
        out,
        "{:<12} {:<18} {:>7} {:>7} {:>8} {:>11}",
        "venue", "market", "n", "invalid", "negative", "mean_ms"
    )?;
    for p in percentiles {
        write!(out, " {:>11}", format!("p{p}_ms"))?;
    }
    writeln!(out, "  status")?;
    let mut failed = false;
    for probe in probes {
        probe.samples.sort_by(f64::total_cmp);
        let samples = &probe.samples;
        let status = if probe.error.is_some() {
            "ERROR"
        } else if samples.is_empty() {
            "NO_DATA"
        } else if probe.invalid > 0 {
            "INVALID"
        } else {
            "OK"
        };
        failed |= status != "OK";
        let market = match &probe.market {
            Some(id) if probe.venue.is_lighter() => format!("{} (id={id})", probe.symbol),
            Some(market) => market.clone(),
            None => probe.symbol.clone(),
        };
        write!(
            out,
            "{:<12} {:<18} {:7} {:7} {:8}",
            probe.venue.name(),
            market,
            samples.len(),
            probe.invalid,
            samples.iter().filter(|v| **v < 0.0).count()
        )?;
        if samples.is_empty() {
            for _ in 0..=percentiles.len() {
                write!(out, " {:>11}", "N/A")?;
            }
        } else {
            // Sum scaled terms so a large finite sample cannot overflow the mean.
            let mean = samples
                .iter()
                .map(|v| v / samples.len() as f64)
                .sum::<f64>();
            write!(out, " {mean:11.3}")?;
            for p in percentiles {
                write!(out, " {:11.3}", percentile(samples, *p))?;
            }
        }
        writeln!(out, "  {status}")?;
        if let Some(error) = &probe.error {
            writeln!(out, "  {error}")?;
        }
    }
    Ok(failed)
}

#[cfg(test)]
mod tests;
