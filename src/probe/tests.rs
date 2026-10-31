use super::*;
use futures_util::future::join_all;
use tokio::{net::TcpListener, time::sleep};
use tokio_tungstenite::{accept_async, accept_hdr_async};

const NOW: f64 = 1_792_577_563.75;
const VENUES: [Venue; 8] = [
    Venue::Hyperliquid,
    Venue::Entropy,
    Venue::Lighter,
    Venue::LighterRh,
    Venue::Tradexyz,
    Venue::Gate,
    Venue::Arcus,
    Venue::Ondo,
];

fn frame(venue: Venue) -> (&'static str, Value, f64) {
    match venue {
        Venue::Hyperliquid | Venue::Entropy | Venue::Tradexyz => (
            "BTC",
            json!({"channel": "l2Book", "data": {"coin": "BTC", "time": 1792577563500u64}}),
            250.0,
        ),
        Venue::Lighter | Venue::LighterRh => (
            "0",
            json!({"type": "update/order_book", "channel": "order_book:0",
            "timestamp": NOW * 1000.0, "order_book": {"last_updated_at": 1792577563500000u64}}),
            250.0,
        ),
        Venue::Gate => (
            "BTC_USDT",
            json!({"channel": "futures.order_book", "event": "all", "time_ms": NOW * 1000.0,
            "result": {"contract": "BTC_USDT", "t": 1792577563500u64}}),
            250.0,
        ),
        Venue::Arcus | Venue::Ondo => {
            let fixtures: Value =
                serde_json::from_str(include_str!("../../tests/fixtures/venue_books.json"))
                    .unwrap();
            if venue == Venue::Arcus {
                ("SNDK-USD", fixtures["arcus"].clone(), 223.812)
            } else {
                ("SNDK-USD.P", fixtures["ondo"].clone(), 257.201)
            }
        }
    }
}

fn prepared(venue: Venue) -> Probe {
    let mut probe = Probe::new(venue, "BTC".to_owned());
    probe.market = Some(frame(venue).0.to_owned());
    probe
}

#[test]
fn wire_timestamps_use_book_clock_and_filter_other_markets() {
    for venue in VENUES {
        let (_, msg, expected_ms) = frame(venue);
        let mut probe = prepared(venue);
        probe.observe(&msg, NOW);
        probe.observe(&json!({"type": "ping"}), NOW);
        assert_eq!(probe.samples.len(), 1, "{venue:?}");
        assert!(
            (probe.samples[0] - expected_ms).abs() < 0.001,
            "{venue:?}: {:?}",
            probe.samples
        );
        probe.market = Some("wrong-market".to_owned());
        probe.observe(&msg, NOW);
        assert_eq!(probe.samples.len(), 1);
        assert_eq!(probe.invalid, 0);
    }
    let mut lighter = prepared(Venue::Lighter);
    let mut msg = frame(Venue::Lighter).1;
    msg["channel"] = json!("order_book/0");
    msg["type"] = json!("subscribed/order_book");
    lighter.observe(&msg, NOW);
    assert_eq!(lighter.samples, [250.0]);
}

#[test]
fn retains_repeats_and_negatives_and_counts_invalid_timestamps() {
    let mut probe = prepared(Venue::Hyperliquid);
    let mut msg = frame(Venue::Hyperliquid).1;
    for now in [NOW, NOW + 1.0, NOW - 1.0] {
        probe.observe(&msg, now);
    }
    for stamp in [
        Value::Null,
        json!("invalid"),
        json!("NaN"),
        json!("inf"),
        json!(0),
        json!(-1),
    ] {
        msg["data"]["time"] = stamp;
        probe.observe(&msg, NOW);
    }
    msg["data"].as_object_mut().unwrap().remove("time");
    probe.observe(&msg, NOW);
    assert_eq!(probe.samples, [250.0, 1250.0, -750.0]);
    assert_eq!(probe.invalid, 7);
    let mut ondo = prepared(Venue::Ondo);
    let mut batch = frame(Venue::Ondo).1;
    batch["data"]
        .as_array_mut()
        .unwrap()
        .push(json!({"market": "SNDK-USD.P", "time": "2026-10-21T10:12:43"}));
    ondo.observe(&batch, NOW);
    assert_eq!(ondo.samples.len(), 1);
    assert_eq!(ondo.invalid, 1);
}

#[test]
fn malformed_book_payloads_are_invalid_samples() {
    for (venue, msg) in [
        (
            Venue::Hyperliquid,
            json!({"channel": "l2Book", "data": null}),
        ),
        (
            Venue::Gate,
            json!({"channel": "futures.order_book", "event": "all", "result": []}),
        ),
        (
            Venue::Ondo,
            json!({"channel": "depthBooksPerps", "type": "update", "data": {}}),
        ),
    ] {
        let mut probe = prepared(venue);
        probe.observe(&msg, NOW);
        assert_eq!(probe.invalid, 1);
        assert!(probe.samples.is_empty());
    }
}

#[test]
fn statistics_interpolate_and_report_partial_missing_and_invalid_data() {
    let mut good = prepared(Venue::Gate);
    good.samples = vec![30.0, 0.0, 20.0, 10.0];
    let mut partial = prepared(Venue::Ondo);
    partial.samples = vec![-1.0];
    partial.error = Some("disconnected".to_owned());
    let empty = prepared(Venue::Arcus);
    let mut invalid = prepared(Venue::Lighter);
    invalid.samples = vec![5.0];
    invalid.invalid = 1;
    let mut output = Vec::new();
    assert!(
        write_results(
            &mut output,
            &mut [good, partial, empty, invalid],
            &[0.0, 50.0, 90.0, 100.0]
        )
        .unwrap()
    );
    let output = String::from_utf8(output).unwrap();
    for text in [
        "15.000",
        "27.000",
        "30.000",
        "ERROR",
        "NO_DATA",
        "INVALID",
        "N/A",
        "disconnected",
        "BTC (id=0)",
    ] {
        assert!(output.contains(text), "{text}: {output}");
    }
    assert_eq!(percentile(&[12.0], 99.9), 12.0);
    let mut good = prepared(Venue::Gate);
    good.samples.push(12.0);
    assert!(!write_results(&mut Vec::new(), &mut [good], &[50.0]).unwrap());
}

#[test]
fn detects_subscription_failures_without_rejecting_success() {
    for msg in [
        json!({"error": "denied"}),
        json!({"success": false}),
        json!({"type": "error"}),
        json!({"type": "degraded"}),
        json!({"type": "unsubscribed"}),
        json!({"channel": "error"}),
        json!({"event": "error"}),
    ] {
        assert!(subscription_failed(&msg));
    }
    for msg in [
        json!({"type": "ping"}),
        json!({"error": null}),
        json!({"error": ""}),
        json!({"success": true}),
        json!({"error": {}}),
    ] {
        assert!(!subscription_failed(&msg));
    }
}

#[tokio::test]
async fn all_venues_subscribe_receive_binary_and_text_answer_pings_and_stop() {
    join_all(VENUES.into_iter().map(|venue| async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            if venue.is_lighter() {
                ws.send(Message::Text(json!({"type": "connected"}).to_string()))
                    .await
                    .unwrap();
            }
            let subscription: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            let mut expected = venue.subscription(frame(venue).0, 0);
            if venue == Venue::Gate {
                assert!(subscription["time"].as_u64().unwrap() > 0);
                expected["time"] = subscription["time"].clone();
            }
            assert_eq!(subscription, expected);
            if venue.is_lighter() {
                ws.send(Message::Text(json!({"type": "ping"}).to_string()))
                    .await
                    .unwrap();
                let pong: Value =
                    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(pong, json!({"type": "pong"}));
            }
            let msg = frame(venue).1.to_string();
            ws.send(Message::Text(msg.clone())).await.unwrap();
            ws.send(Message::Binary(msg.into_bytes())).await.unwrap();
            ws.send(Message::Ping(b"server".to_vec())).await.unwrap();
            assert_eq!(
                ws.next().await.unwrap().unwrap(),
                Message::Pong(b"server".to_vec())
            );
            // The deadline drops the socket; the peer must see closure, not a leaked task.
            let closed = timeout(Duration::from_secs(2), ws.next()).await.unwrap();
            assert!(matches!(
                closed,
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        });
        let mut probe = prepared(venue);
        probe
            .run_at(&url, Instant::now() + Duration::from_millis(300))
            .await;
        assert!(probe.error.is_none(), "{:?}", probe.error);
        assert_eq!(probe.samples.len(), 2, "{venue:?}");
        assert_eq!(probe.invalid, 0);
        server.await.unwrap();
    }))
    .await;
}

#[tokio::test]
async fn early_close_preserves_samples_and_reports_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(frame(Venue::Gate).1.to_string()))
            .await
            .unwrap();
        ws.close(None).await.unwrap();
    });
    let mut probe = prepared(Venue::Gate);
    probe
        .run_at(&url, Instant::now() + Duration::from_secs(2))
        .await;
    assert_eq!(probe.samples.len(), 1);
    assert!(
        probe
            .error
            .unwrap()
            .contains("closed before measurement ended")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn rejected_handshake_includes_exchange_reason() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let result = accept_hdr_async(socket, |_: &_, _| {
            Err(tokio_tungstenite::tungstenite::http::Response::builder()
                .status(400)
                .body(Some("region restricted".to_owned()))
                .unwrap())
        })
        .await;
        assert!(result.is_err());
    });
    let mut probe = prepared(Venue::Lighter);
    probe
        .run_at(&url, Instant::now() + Duration::from_secs(2))
        .await;
    let error = probe.error.unwrap();
    assert!(
        error.contains("400") && error.contains("region restricted"),
        "{error}"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_json_and_subscription_rejection_are_reported() {
    for raw in [
        "not json",
        "[]",
        r#"{"success":false,"error":"market missing"}"#,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(raw.to_owned())).await.unwrap();
            let _ = ws.next().await;
        });
        let mut probe = prepared(Venue::Gate);
        probe
            .run_at(&url, Instant::now() + Duration::from_secs(2))
            .await;
        assert!(probe.error.is_some());
        assert!(probe.samples.is_empty());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn shared_deadline_bounds_a_stalled_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        sleep(Duration::from_secs(10)).await;
    });
    let mut stalled = prepared(Venue::Gate);
    let start = Instant::now();
    stalled
        .run_at(&url, start + Duration::from_millis(50))
        .await;
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(stalled.error.is_none());
    assert!(stalled.samples.is_empty());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn one_failed_stream_does_not_cancel_another() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws.next().await.unwrap().unwrap();
        sleep(Duration::from_millis(50)).await;
        ws.send(Message::Text(frame(Venue::Gate).1.to_string()))
            .await
            .unwrap();
        let _ = ws.next().await;
    });
    let mut good = prepared(Venue::Gate);
    let mut failed = prepared(Venue::Ondo);
    let deadline = Instant::now() + Duration::from_millis(300);
    tokio::join!(
        good.run_at(&url, deadline),
        failed.run_at("invalid://url", deadline)
    );
    assert_eq!(good.samples.len(), 1);
    assert!(good.error.is_none());
    assert!(failed.error.is_some());
    server.await.unwrap();
}

#[tokio::test]
async fn application_heartbeat_uses_current_gate_time() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let subscription: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        let ping: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(ping["channel"], "futures.ping");
        assert!(ping["time"].as_u64().unwrap() > subscription["time"].as_u64().unwrap());
        ws.close(None).await.unwrap();
    });
    let mut probe = prepared(Venue::Gate);
    probe
        .run_at(&url, Instant::now() + Duration::from_secs(7))
        .await;
    server.await.unwrap();
}
