//! In-process integration with the Router pipeline, through the crate's
//! `TestHarness`: deduplication (on, and off in one test), WebSocket passthrough
//! to the Node subgraphs and a real multipart response, with the plugin loaded
//! from the configuration. The `TestHarness` uses an unrestricted license in tests only; it does not replace
//! running the binary with GraphOS (`npm run test:router-scale`).
//!
//! Needs the subgraphs from `testbed/` running; `npm run test:router-harness` (in
//! `testbed/`) starts everything and runs:
//!   SUPERGRAPH=... SCORES_URL=http://127.0.0.1:4001 cargo test --release -- --ignored

use std::time::Duration;
use std::time::Instant;

use apollo_router::Context;
use apollo_router::TestHarness;
use apollo_router::services::router;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use http_body_util::BodyExt;
use serde_json::Value;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

// Linking the library registers the plugin in the Router plugin registry.
use subscription_snapshot as _;

const SCOREBOARD: &str = "subscription Scoreboard($matchId: ID!) { scoreboardUpdates(matchId: $matchId) { matchId version state { status home away } } }";

fn env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("set {name} (or use npm run test:router-harness)"))
}

async fn harness(snapshot: bool) -> router::BoxCloneService {
    harness_with(snapshot, true).await
}

async fn harness_with(snapshot: bool, deduplication: bool) -> router::BoxCloneService {
    let supergraph = std::fs::read_to_string(env("SUPERGRAPH")).unwrap();
    let ws = json!({ "path": "/graphql", "protocol": "graphql_ws", "heartbeat_interval": "5s" });
    let passthrough = json!({ "scores": ws, "stats": ws });
    TestHarness::builder()
        .schema(&supergraph)
        .with_subgraph_network_requests()
        .configuration_json(json!({
            "subscription": { "enabled": true, "deduplication": { "enabled": deduplication }, "mode": { "passthrough": { "subgraphs": passthrough } } },
            "headers": { "all": { "request": [{ "propagate": { "named": "authorization" } }] } },
            "include_subgraph_errors": { "all": true },
            "plugins": { "globo.subscription_snapshot": { "enabled": snapshot, "identity_headers": ["authorization"] } }
        }))
        .unwrap()
        .build_router()
        .await
        .unwrap()
}

fn request(body: Value, token: Option<&str>) -> router::Request {
    let mut http = http::Request::builder()
        .method("POST")
        .uri("http://router/graphql")
        .header("content-type", "application/json")
        .header(
            "accept",
            "multipart/mixed;subscriptionSpec=\"1.0\", application/json",
        );
    if let Some(token) = token {
        http = http.header("authorization", token);
    }
    let http = http
        .body(router::body::from_bytes(body.to_string()))
        .unwrap();
    router::Request::from((http, Context::new()))
}

async fn mutate(router: &router::BoxCloneService, query: &str, match_id: &str) -> Value {
    use tower::ServiceExt;
    let response = router
        .clone()
        .oneshot(request(
            json!({ "query": query, "variables": { "m": match_id } }),
            None,
        ))
        .await
        .unwrap();
    let bytes = response
        .response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body.get("errors").is_none(), "mutation failed: {body}");
    body["data"].clone()
}

/// Payloads of a multipart response (without heartbeats).
async fn subscribe(
    router: &router::BoxCloneService,
    match_id: &str,
    token: Option<&str>,
) -> impl Stream<Item = Value> + Unpin {
    use tower::ServiceExt;
    let response = router
        .clone()
        .oneshot(request(json!({ "query": SCOREBOARD, "operationName": "Scoreboard", "variables": { "matchId": match_id } }), token))
        .await
        .unwrap();
    let body = response.response.into_body();
    let chunks = body
        .into_data_stream()
        .map(|chunk| chunk.unwrap_or_else(|_| Bytes::new()));
    let mut buffer = String::new();
    chunks
        .flat_map(move |chunk| {
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            // Each part: headers, blank line, JSON, "\r\n--graphql".
            let mut payloads = Vec::new();
            while let Some(headers_end) = buffer.find("\r\n\r\n") {
                let start = headers_end + 4;
                let Some(len) = buffer[start..].find("\r\n--graphql") else {
                    break;
                };
                if let Ok(part) = serde_json::from_str::<Value>(&buffer[start..start + len])
                    && part.get("payload").is_some_and(|p| !p.is_null())
                {
                    payloads.push(part["payload"].clone());
                }
                buffer.drain(..start + len + "\r\n--graphql".len());
            }
            futures::stream::iter(payloads)
        })
        .boxed()
}

async fn next_frame(stream: &mut (impl Stream<Item = Value> + Unpin)) -> Value {
    timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("no frame within 10 s")
        .expect("the stream closed")
}

fn version(payload: &Value) -> i64 {
    payload["data"]["scoreboardUpdates"]["version"]
        .as_i64()
        .unwrap_or_else(|| panic!("frame without a version: {payload}"))
}
fn is_snapshot(payload: &Value) -> bool {
    payload["extensions"]["snapshot"] == json!(true)
}

/// Streams open in the scores subgraph for the match (GET /metrics).
async fn upstreams(match_id: &str) -> i64 {
    let base = env("SCORES_URL");
    let addr = base.trim_start_matches("http://");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(
        format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut raw = String::new();
    tcp.read_to_string(&mut raw).await.unwrap();
    let body: Value = serde_json::from_str(&raw[raw.find("\r\n\r\n").unwrap() + 4..]).unwrap();
    body["scoreboard"]["activeByMatch"][match_id]
        .as_i64()
        .unwrap_or(0)
}

async fn until_upstreams(match_id: &str, expected: i64) {
    let end = Instant::now() + Duration::from_secs(10);
    while upstreams(match_id).await != expected {
        assert!(
            Instant::now() < end,
            "upstreams of {match_id}: expected {expected}, got {}",
            upstreams(match_id).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

const START: &str = "mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }";
const GOAL: &str = "mutation($m: ID!) { scoreGoal(matchId: $m, side: HOME) { version } }";
const FINISH: &str = "mutation($m: ID!) { endPeriod(matchId: $m, final: true) { version } }";

/// The client stream ends (the multipart response closes) within 5 s, with no more frames with data.
async fn ends(stream: &mut (impl Stream<Item = Value> + Unpin)) -> Vec<Value> {
    let mut rest = Vec::new();
    loop {
        match timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("the stream did not end within 5 s")
        {
            Some(frame) => rest.push(frame),
            None => return rest,
        }
    }
}
fn status(payload: &Value) -> &str {
    payload["data"]["scoreboardUpdates"]["state"]["status"]
        .as_str()
        .unwrap_or("")
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the subgraphs (npm run test:router-harness)"]
async fn deduplication_and_snapshot_in_the_router_pipeline() {
    let router = harness(true).await;
    let match_id = unique("harness");
    mutate(&router, START, &match_id).await;

    let mut a = subscribe(&router, &match_id, None).await;
    let first = next_frame(&mut a).await;
    assert!(
        !is_snapshot(&first),
        "the first subscriber gets the initial frame from the subgraph"
    );
    until_upstreams(&match_id, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await; // upstream open and idle

    let t0 = Instant::now();
    let mut b = subscribe(&router, &match_id, None).await;
    let late = next_frame(&mut b).await;
    let late_ms = t0.elapsed().as_millis();
    println!("late joiner with snapshot: {late_ms} ms, frame {late}");
    assert!(
        is_snapshot(&late),
        "the late joiner gets the snapshot: {late}"
    );
    assert_eq!(version(&late), version(&first));
    assert!(late_ms < 300, "with snapshot: {late_ms} ms");
    assert_eq!(
        upstreams(&match_id).await,
        1,
        "the Router deduplication kept one upstream for both"
    );

    // After the snapshot, `b` keeps receiving the live events of the shared upstream.
    let goal = mutate(&router, GOAL, &match_id).await["scoreGoal"]["version"]
        .as_i64()
        .unwrap();
    for stream in [&mut a, &mut b] {
        loop {
            let frame = next_frame(stream).await;
            if version(&frame) >= goal {
                assert!(!is_snapshot(&frame));
                assert_eq!(
                    frame["data"]["scoreboardUpdates"]["state"]["home"],
                    json!(1)
                );
                break;
            }
        }
    }

    drop(a);
    drop(b);
    until_upstreams(&match_id, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the subgraphs (npm run test:router-harness)"]
async fn without_snapshot_the_late_joiner_waits_for_the_periodic_frame() {
    let router = harness(false).await;
    let match_id = unique("harness-without");
    mutate(&router, START, &match_id).await;
    let mut a = subscribe(&router, &match_id, None).await;
    next_frame(&mut a).await;
    until_upstreams(&match_id, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let t0 = Instant::now();
    let mut b = subscribe(&router, &match_id, None).await;
    let late = next_frame(&mut b).await;
    let late_ms = t0.elapsed().as_millis();
    println!("late joiner without snapshot: {late_ms} ms");
    assert!(!is_snapshot(&late));
    assert!(
        late_ms > 500,
        "without snapshot, it waits for the periodic frame ({late_ms} ms)"
    );
    assert_eq!(upstreams(&match_id).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the subgraphs (npm run test:router-harness)"]
async fn without_deduplication_the_late_joiner_gets_the_snapshot_and_its_own_upstream() {
    let router = harness_with(true, false).await;
    let match_id = unique("harness-no-dedup");
    mutate(&router, START, &match_id).await;
    let mut a = subscribe(&router, &match_id, None).await;
    let first = next_frame(&mut a).await;
    assert!(!is_snapshot(&first));
    until_upstreams(&match_id, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The snapshot goes right after the marker, before anything from the new upstream.
    let mut b = subscribe(&router, &match_id, None).await;
    let late = next_frame(&mut b).await;
    assert!(
        is_snapshot(&late),
        "the snapshot does not depend on deduplication: {late}"
    );
    assert_eq!(version(&late), version(&first));
    until_upstreams(&match_id, 2).await;
    let own = next_frame(&mut b).await;
    assert!(
        !is_snapshot(&own),
        "then the initial frame of its own upstream: {own}"
    );
    assert_eq!(version(&own), version(&first));

    let goal = mutate(&router, GOAL, &match_id).await["scoreGoal"]["version"]
        .as_i64()
        .unwrap();
    for stream in [&mut a, &mut b] {
        while version(&next_frame(stream).await) < goal {}
    }

    drop(a);
    drop(b);
    until_upstreams(&match_id, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the subgraphs (npm run test:router-harness)"]
async fn different_identities_share_neither_upstream_nor_snapshot() {
    let router = harness(true).await;
    let match_id = unique("harness-identity");
    mutate(&router, START, &match_id).await;
    let mut a1 = subscribe(&router, &match_id, Some("Bearer a")).await;
    next_frame(&mut a1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = subscribe(&router, &match_id, Some("Bearer b")).await;
    let mut a2 = subscribe(&router, &match_id, Some("Bearer a")).await;
    let (fb, fa2) = (next_frame(&mut b).await, next_frame(&mut a2).await);
    assert!(
        !is_snapshot(&fb),
        "`b` (another token) does not get the frame of `a1`"
    );
    assert!(
        is_snapshot(&fa2),
        "`a2` (same token as `a1`) gets the snapshot"
    );
    until_upstreams(&match_id, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the subgraphs (npm run test:router-harness)"]
async fn full_time_ends_the_deduplicated_clients_and_clears_the_snapshot() {
    let router = harness(true).await;
    let match_id = unique("harness-end");
    mutate(&router, START, &match_id).await;
    let mut a = subscribe(&router, &match_id, None).await;
    next_frame(&mut a).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = subscribe(&router, &match_id, None).await;
    assert!(is_snapshot(&next_frame(&mut b).await));
    assert_eq!(upstreams(&match_id).await, 1);

    // The subgraph sends the FULL_TIME frame and ends; the Router closes both clients.
    mutate(&router, FINISH, &match_id).await;
    for stream in [&mut a, &mut b] {
        let rest = ends(stream).await;
        let last = rest
            .iter()
            .rev()
            .find(|f| f["data"].is_object())
            .expect("FULL_TIME frame before the end");
        assert_eq!(status(last), "FULL_TIME");
    }
    until_upstreams(&match_id, 0).await;

    // A client that arrives after the end gets the final state from the subgraph (not a
    // snapshot of an upstream that already closed) and its stream ends right after.
    let mut c = subscribe(&router, &match_id, None).await;
    let first = next_frame(&mut c).await;
    assert!(
        !is_snapshot(&first),
        "the snapshot was cleared with the upstream"
    );
    assert_eq!(status(&first), "FULL_TIME");
    ends(&mut c).await;
    until_upstreams(&match_id, 0).await;
}
