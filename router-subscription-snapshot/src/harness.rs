//! Integração no pipeline do Router, em processo, com o `TestHarness` do crate:
//! deduplicação, passthrough por WebSocket para os subgraphs Node e multipart de
//! verdade, com o plugin carregado pela config. O `TestHarness` usa uma licença
//! irrestrita só em testes; não substitui rodar o binário com GraphOS
//! (`npm run test:router-scale`).
//!
//! Precisa dos subgraphs rodando; `npm run test:router-harness` sobe tudo e roda:
//!   SUPERGRAPH=... SCORES_URL=http://127.0.0.1:4001 cargo test --release -- --ignored

use std::time::Duration;
use std::time::Instant;

use apollo_router::services::router;
use apollo_router::Context;
use apollo_router::TestHarness;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use http_body_util::BodyExt;
use serde_json::json;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const SCOREBOARD: &str = "subscription Scoreboard($matchId: ID!) { scoreboardUpdates(matchId: $matchId) { matchId version state { status home away } } }";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("defina {name} (ou use npm run test:router-harness)"))
}

async fn harness(retain: bool) -> router::BoxCloneService {
    let supergraph = std::fs::read_to_string(env("SUPERGRAPH")).unwrap();
    let ws = json!({ "path": "/graphql", "protocol": "graphql_ws", "heartbeat_interval": "5s" });
    let passthrough = json!({ "scores": ws, "stats": ws });
    TestHarness::builder()
        .schema(&supergraph)
        .with_subgraph_network_requests()
        .configuration_json(json!({
            "subscription": { "enabled": true, "deduplication": { "enabled": true }, "mode": { "passthrough": { "subgraphs": passthrough } } },
            "headers": { "all": { "request": [{ "propagate": { "named": "authorization" } }] } },
            "include_subgraph_errors": { "all": true },
            "plugins": { "subscription_snapshot": { "enabled": retain, "identity_headers": ["authorization"] } }
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
        .header("accept", "multipart/mixed;subscriptionSpec=\"1.0\", application/json");
    if let Some(token) = token {
        http = http.header("authorization", token);
    }
    let http = http.body(router::body::from_bytes(body.to_string())).unwrap();
    router::Request::from((http, Context::new()))
}

async fn mutate(router: &router::BoxCloneService, query: &str, match_id: &str) -> Value {
    use tower::ServiceExt;
    let response = router.clone().oneshot(request(json!({ "query": query, "variables": { "m": match_id } }), None)).await.unwrap();
    let bytes = response.response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(body.get("errors").is_none(), "mutation falhou: {body}");
    body["data"].clone()
}

/// Payloads de uma resposta multipart (sem heartbeats).
async fn subscribe(router: &router::BoxCloneService, match_id: &str, token: Option<&str>) -> impl Stream<Item = Value> + Unpin {
    use tower::ServiceExt;
    let response = router
        .clone()
        .oneshot(request(json!({ "query": SCOREBOARD, "operationName": "Scoreboard", "variables": { "matchId": match_id } }), token))
        .await
        .unwrap();
    let body = response.response.into_body();
    let chunks = body.into_data_stream().map(|chunk| chunk.unwrap_or_else(|_| Bytes::new()));
    let mut buffer = String::new();
    chunks
        .flat_map(move |chunk| {
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            // Cada parte: cabeçalhos, linha em branco, JSON, "\r\n--graphql".
            let mut payloads = Vec::new();
            while let Some(headers_end) = buffer.find("\r\n\r\n") {
                let start = headers_end + 4;
                let Some(len) = buffer[start..].find("\r\n--graphql") else { break };
                if let Ok(part) = serde_json::from_str::<Value>(&buffer[start..start + len]) {
                    if part.get("payload").is_some_and(|p| !p.is_null()) {
                        payloads.push(part["payload"].clone());
                    }
                }
                buffer.drain(..start + len + "\r\n--graphql".len());
            }
            futures::stream::iter(payloads)
        })
        .boxed()
}

async fn next_frame(stream: &mut (impl Stream<Item = Value> + Unpin)) -> Value {
    timeout(Duration::from_secs(10), stream.next()).await.expect("sem frame em 10 s").expect("stream fechou")
}

fn version(payload: &Value) -> i64 {
    payload["data"]["scoreboardUpdates"]["version"].as_i64().unwrap_or_else(|| panic!("frame sem versão: {payload}"))
}
fn is_snapshot(payload: &Value) -> bool {
    payload["extensions"]["snapshot"] == json!(true)
}

/// Streams abertos no subgraph de placar para a partida (GET /metrics).
async fn upstreams(match_id: &str) -> i64 {
    let base = env("SCORES_URL");
    let addr = base.trim_start_matches("http://");
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    tcp.write_all(format!("GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut raw = String::new();
    tcp.read_to_string(&mut raw).await.unwrap();
    let body: Value = serde_json::from_str(&raw[raw.find("\r\n\r\n").unwrap() + 4..]).unwrap();
    body["scoreboard"]["activeByMatch"][match_id].as_i64().unwrap_or(0)
}

async fn until_upstreams(match_id: &str, expected: i64) {
    let end = Instant::now() + Duration::from_secs(10);
    while upstreams(match_id).await != expected {
        assert!(Instant::now() < end, "upstreams de {match_id}: esperava {expected}, veio {}", upstreams(match_id).await);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

const START: &str = "mutation($m: ID!) { startPeriod(matchId: $m, period: 1) { version } }";
const GOAL: &str = "mutation($m: ID!) { scoreGoal(matchId: $m, side: HOME) { version } }";
const FINISH: &str = "mutation($m: ID!) { endPeriod(matchId: $m, final: true) { version } }";

/// O stream do cliente termina (o multipart fecha) em até 5 s, sem mais frames com dados.
async fn ends(stream: &mut (impl Stream<Item = Value> + Unpin)) -> Vec<Value> {
    let mut rest = Vec::new();
    loop {
        match timeout(Duration::from_secs(5), stream.next()).await.expect("o stream não terminou em 5 s") {
            Some(frame) => rest.push(frame),
            None => return rest,
        }
    }
}
fn status(payload: &Value) -> &str {
    payload["data"]["scoreboardUpdates"]["state"]["status"].as_str().unwrap_or("")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "precisa dos subgraphs (npm run test:router-harness)"]
async fn deduplicacao_e_retencao_no_pipeline_do_router() {
    let router = harness(true).await;
    let match_id = unique("harness");
    mutate(&router, START, &match_id).await;

    let mut a = subscribe(&router, &match_id, None).await;
    let first = next_frame(&mut a).await;
    assert!(!is_snapshot(&first), "o primeiro assinante recebe o frame inicial do subgraph");
    until_upstreams(&match_id, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await; // upstream aberto e parado

    let t0 = Instant::now();
    let mut b = subscribe(&router, &match_id, None).await;
    let late = next_frame(&mut b).await;
    let late_ms = t0.elapsed().as_millis();
    println!("late joiner com retenção: {late_ms} ms, frame {late}");
    assert!(is_snapshot(&late), "o late joiner recebe o frame retido: {late}");
    assert_eq!(version(&late), version(&first));
    assert!(late_ms < 300, "com retenção: {late_ms} ms");
    assert_eq!(upstreams(&match_id).await, 1, "a deduplicação do Router manteve um upstream para os dois");

    // Depois do frame retido, B continua recebendo os eventos ao vivo do upstream compartilhado.
    let goal = mutate(&router, GOAL, &match_id).await["scoreGoal"]["version"].as_i64().unwrap();
    for stream in [&mut a, &mut b] {
        loop {
            let frame = next_frame(stream).await;
            if version(&frame) >= goal {
                assert!(!is_snapshot(&frame));
                assert_eq!(frame["data"]["scoreboardUpdates"]["state"]["home"], json!(1));
                break;
            }
        }
    }

    drop(a);
    drop(b);
    until_upstreams(&match_id, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "precisa dos subgraphs (npm run test:router-harness)"]
async fn sem_retencao_o_late_joiner_espera_o_periodico() {
    let router = harness(false).await;
    let match_id = unique("harness-sem");
    mutate(&router, START, &match_id).await;
    let mut a = subscribe(&router, &match_id, None).await;
    next_frame(&mut a).await;
    until_upstreams(&match_id, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let t0 = Instant::now();
    let mut b = subscribe(&router, &match_id, None).await;
    let late = next_frame(&mut b).await;
    let late_ms = t0.elapsed().as_millis();
    println!("late joiner sem retenção: {late_ms} ms");
    assert!(!is_snapshot(&late));
    assert!(late_ms > 500, "sem retenção, espera o frame periódico ({late_ms} ms)");
    assert_eq!(upstreams(&match_id).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "precisa dos subgraphs (npm run test:router-harness)"]
async fn identidades_diferentes_nao_compartilham_upstream_nem_frame() {
    let router = harness(true).await;
    let match_id = unique("harness-identidade");
    mutate(&router, START, &match_id).await;
    let mut a1 = subscribe(&router, &match_id, Some("Bearer a")).await;
    next_frame(&mut a1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = subscribe(&router, &match_id, Some("Bearer b")).await;
    let mut a2 = subscribe(&router, &match_id, Some("Bearer a")).await;
    let (fb, fa2) = (next_frame(&mut b).await, next_frame(&mut a2).await);
    assert!(!is_snapshot(&fb), "b (outro token) não recebe o frame de a");
    assert!(is_snapshot(&fa2), "a2 (mesmo token de a1) recebe o frame retido");
    until_upstreams(&match_id, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "precisa dos subgraphs (npm run test:router-harness)"]
async fn full_time_encerra_os_clientes_deduplicados_e_limpa_a_retencao() {
    let router = harness(true).await;
    let match_id = unique("harness-fim");
    mutate(&router, START, &match_id).await;
    let mut a = subscribe(&router, &match_id, None).await;
    next_frame(&mut a).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut b = subscribe(&router, &match_id, None).await;
    assert!(is_snapshot(&next_frame(&mut b).await));
    assert_eq!(upstreams(&match_id).await, 1);

    // O subgraph manda o frame FULL_TIME e encerra; o Router fecha os dois clientes.
    mutate(&router, FINISH, &match_id).await;
    for stream in [&mut a, &mut b] {
        let rest = ends(stream).await;
        let last = rest.iter().rev().find(|f| f["data"].is_object()).expect("frame FULL_TIME antes do fim");
        assert_eq!(status(last), "FULL_TIME");
    }
    until_upstreams(&match_id, 0).await;

    // Quem chega depois do fim recebe o estado final do subgraph (não um frame retido
    // de um upstream que já fechou) e o stream termina em seguida.
    let mut c = subscribe(&router, &match_id, None).await;
    let first = next_frame(&mut c).await;
    assert!(!is_snapshot(&first), "a retenção foi limpa com o upstream");
    assert_eq!(status(&first), "FULL_TIME");
    ends(&mut c).await;
    until_upstreams(&match_id, 0).await;
}
