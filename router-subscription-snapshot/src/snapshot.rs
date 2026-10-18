//! `subscription_snapshot`: guarda o último evento de cada subscription deduplicada
//! (o snapshot) e o entrega a quem entra num upstream já aberto, logo depois do
//! marcador de início, marcado com `extensions.snapshot: true`.
//!
//! O stream que sai do supergraph service numa subscription é:
//!   1. um marcador (`subscribed == Some(true)`, sem dados), que o router service
//!      consome para decidir abrir o multipart e não envia ao cliente;
//!   2. os eventos (`subscribed == Some(true)`, com dados);
//!   3. um item final vazio (`subscribed == Some(false)`), que fecha o multipart.
//! Por isso o frame retido entra depois do item 1: inserido antes, ele viraria o
//! marcador e se perderia.
//!
//! O assinante sai do Store no fim do stream ou quando o cliente é descartado, o
//! que vier primeiro.
//!
//! Só é correto porque todo frame traz o estado inteiro e uma versão: um frame
//! retido mais velho que um em trânsito é ignorado pelo cliente.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use apollo_router::graphql;
use apollo_router::layers::ServiceExt as _;
use apollo_router::plugin::Plugin;
use apollo_router::plugin::PluginInit;
use apollo_router::register_plugin;
use apollo_router::services::supergraph;
use futures::stream;
use futures::FutureExt;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::Deserialize;
use tower::BoxError;
use tower::ServiceExt;

const OPERATION_KIND: &str = "apollo::supergraph::operation_kind";
/// Extensão adicionada ao snapshot, para que testes e clientes o distingam.
pub const SNAPSHOT_EXTENSION: &str = "snapshot";

#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Liga a retenção. Desligado, o plugin não toca nas respostas.
    pub enabled: bool,
    /// Headers que entram na chave de retenção (os mesmos que a deduplicação do
    /// Router não ignora, ex.: `authorization`). Identidades diferentes nunca
    /// compartilham frame retido.
    pub identity_headers: Vec<String>,
}

struct Entry<T> {
    last: Option<T>,
    subscribers: usize,
}

/// Último frame por chave, vivo enquanto houver assinante com essa chave.
pub struct Store<T> {
    entries: Mutex<HashMap<String, Entry<T>>>,
}

impl<T: Clone> Default for Store<T> {
    fn default() -> Self {
        Self { entries: Mutex::new(HashMap::new()) }
    }
}

impl<T: Clone> Store<T> {
    /// Registra um assinante e devolve o frame retido para ele, se houver.
    pub fn join(self: &Arc<Self>, key: &str) -> (Option<T>, Subscription<T>) {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(key.to_string()).or_insert(Entry { last: None, subscribers: 0 });
        entry.subscribers += 1;
        (entry.last.clone(), Subscription { store: self.clone(), key: key.to_string() })
    }

    fn retain(&self, key: &str, frame: &T) {
        if let Some(entry) = self.entries.lock().unwrap().get_mut(key) {
            entry.last = Some(frame.clone());
        }
    }

    fn leave(&self, key: &str) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get_mut(key) {
            entry.subscribers -= 1;
            if entry.subscribers == 0 {
                entries.remove(key);
            }
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn retained(&self, key: &str) -> Option<T> {
        self.entries.lock().unwrap().get(key).and_then(|e| e.last.clone())
    }
}

/// Um assinante; sai do `Store` quando o stream do cliente é descartado.
pub struct Subscription<T: Clone> {
    store: Arc<Store<T>>,
    key: String,
}

impl<T: Clone> Subscription<T> {
    fn retain(&self, frame: &T) {
        self.store.retain(&self.key, frame);
    }
}

impl<T: Clone> Drop for Subscription<T> {
    fn drop(&mut self) {
        self.store.leave(&self.key);
    }
}

/// Chave: documento, nome da operação, variáveis e os headers de identidade.
pub fn snapshot_key(request: &http::Request<graphql::Request>, identity_headers: &[String]) -> String {
    let body = request.body();
    let identity: Vec<(&str, &str)> = identity_headers
        .iter()
        .map(|name| {
            let value = request.headers().get(name.as_str()).and_then(|v| v.to_str().ok()).unwrap_or("");
            (name.as_str(), value)
        })
        .collect();
    serde_json::json!([body.query, body.operation_name, body.variables, identity]).to_string()
}

fn is_event(frame: &graphql::Response) -> bool {
    frame.subscribed == Some(true) && frame.data.is_some() && frame.errors.is_empty()
}

/// Insere o frame retido depois do marcador e guarda cada evento novo.
/// Streams que não começam com um marcador de subscription aberta passam intactos.
pub fn snapshot_stream(
    responses: graphql::ResponseStream,
    store: Arc<Store<graphql::Response>>,
    key: String,
) -> graphql::ResponseStream {
    responses
        .into_future()
        .map(move |(first, rest)| {
            let Some(first) = first else { return stream::empty().boxed() };
            if first.subscribed != Some(true) || !first.errors.is_empty() {
                return stream::once(async { first }).chain(rest).boxed();
            }
            let (retained, subscription) = store.join(&key);
            let retained = retained.map(|mut frame| {
                frame.extensions.insert(SNAPSHOT_EXTENSION, true.into());
                frame
            });
            // O assinante sai do Store quando o stream termina (item final com
            // subscribed: false, ou fim do stream), não só quando o cliente é
            // descartado: quem chega depois do fim do upstream não pode receber um
            // frame retido dele.
            let events = stream::unfold((rest, Some(subscription)), |(mut rest, mut subscription)| async move {
                let frame = rest.next().await?;
                if frame.subscribed == Some(false) {
                    subscription = None;
                } else if let Some(subscription) = &subscription {
                    if is_event(&frame) {
                        subscription.retain(&frame);
                    }
                }
                Some((frame, (rest, subscription)))
            });
            stream::once(async { first }).chain(stream::iter(retained)).chain(events).boxed()
        })
        .flatten_stream()
        .boxed()
}

pub struct SubscriptionSnapshot {
    config: Config,
    store: Arc<Store<graphql::Response>>,
}

#[async_trait::async_trait]
impl Plugin for SubscriptionSnapshot {
    type Config = Config;

    async fn new(init: PluginInit<Self::Config>) -> Result<Self, BoxError> {
        Ok(Self { config: init.config, store: Arc::default() })
    }

    fn supergraph_service(&self, service: supergraph::BoxService) -> supergraph::BoxService {
        if !self.config.enabled {
            return service;
        }
        let headers = self.config.identity_headers.clone();
        let store = self.store.clone();
        service
            .map_future_with_request_data(
                move |req: &supergraph::Request| snapshot_key(&req.supergraph_request, &headers),
                move |key: String, response| {
                    let store = store.clone();
                    async move {
                        let response: supergraph::Response = response.await?;
                        let kind = response.context.get::<_, String>(OPERATION_KIND).ok().flatten();
                        if kind.as_deref() != Some("subscription") {
                            return Ok(response);
                        }
                        Ok(response.map(move |responses| snapshot_stream(responses, store, key)))
                    }
                },
            )
            .boxed()
    }
}

register_plugin!("", "subscription_snapshot", SubscriptionSnapshot);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json_bytes::json;

    fn marker() -> graphql::Response {
        graphql::Response::builder().subscribed(true).build()
    }
    fn event(version: i64) -> graphql::Response {
        graphql::Response::builder()
            .data(json!({ "scoreboardUpdates": { "version": version } }))
            .subscribed(true)
            .build()
    }
    fn end() -> graphql::Response {
        graphql::Response::builder().subscribed(false).build()
    }
    fn version(frame: &graphql::Response) -> Option<i64> {
        frame.data.as_ref()?.get("scoreboardUpdates")?.get("version")?.as_i64()
    }
    fn is_snapshot(frame: &graphql::Response) -> bool {
        frame.extensions.get(SNAPSHOT_EXTENSION) == Some(&true.into())
    }
    fn upstream(frames: Vec<graphql::Response>) -> graphql::ResponseStream {
        stream::iter(frames).boxed()
    }

    #[tokio::test]
    async fn primeiro_assinante_nao_recebe_frame_retido_e_alimenta_o_store() {
        let store = Arc::<Store<graphql::Response>>::default();
        let mut s = snapshot_stream(upstream(vec![marker(), event(1), event(2)]), store.clone(), "k".into());
        let first = s.next().await.unwrap();
        assert_eq!(first.subscribed, Some(true));
        assert!(first.data.is_none(), "o marcador passa intacto");
        assert_eq!(version(&s.next().await.unwrap()), Some(1));
        assert_eq!(version(&s.next().await.unwrap()), Some(2));
        assert_eq!(store.retained("k").as_ref().and_then(version), Some(2));
    }

    #[tokio::test]
    async fn late_joiner_recebe_o_frame_retido_logo_depois_do_marcador() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (pending_tx, pending_rx) = futures::channel::mpsc::unbounded();
        let mut a = snapshot_stream(stream::iter(vec![marker(), event(7)]).chain(pending_rx).boxed(), store.clone(), "k".into());
        a.next().await;
        a.next().await; // a viu a versão 7

        let mut b = snapshot_stream(upstream(vec![marker(), event(8)]), store.clone(), "k".into());
        let frames: Vec<_> = (&mut b).collect().await;
        assert!(frames[0].data.is_none() && frames[0].subscribed == Some(true), "marcador primeiro");
        assert!(is_snapshot(&frames[1]));
        assert_eq!(version(&frames[1]), Some(7));
        assert_eq!(frames[1].subscribed, Some(true), "o retido mantém o multipart aberto");
        assert_eq!(version(&frames[2]), Some(8));
        assert!(!is_snapshot(&frames[2]));
        drop(pending_tx);
    }

    #[tokio::test]
    async fn chaves_diferentes_nao_compartilham_frame() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (_tx, rx) = futures::channel::mpsc::unbounded();
        let mut a = snapshot_stream(stream::iter(vec![marker(), event(3)]).chain(rx).boxed(), store.clone(), "token-a".into());
        a.next().await;
        a.next().await;
        let frames: Vec<_> = snapshot_stream(upstream(vec![marker()]), store.clone(), "token-b".into()).collect().await;
        assert_eq!(frames.len(), 1);
        assert!(frames.iter().all(|f| !is_snapshot(f)));
    }

    #[tokio::test]
    async fn erros_nao_sao_retidos_e_o_item_final_libera() {
        let store = Arc::<Store<graphql::Response>>::default();
        let error = graphql::Response::builder()
            .data(json!({ "scoreboardUpdates": null }))
            .error(graphql::Error::builder().message("boom").extension_code("X").build())
            .subscribed(true)
            .build();
        let (_tx, rx) = futures::channel::mpsc::unbounded();
        let mut a = snapshot_stream(stream::iter(vec![marker(), event(1), error, end()]).chain(rx).boxed(), store.clone(), "k".into());
        for _ in 0..3 {
            a.next().await;
        }
        assert_eq!(store.retained("k").as_ref().and_then(version), Some(1), "o erro não substitui o retido");
        a.next().await;
        assert_eq!(store.len(), 0, "o item final libera a entrada");
    }

    #[tokio::test]
    async fn sem_assinantes_o_frame_retido_some() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (_tx1, rx1) = futures::channel::mpsc::unbounded();
        let (_tx2, rx2) = futures::channel::mpsc::unbounded();
        let mut a = snapshot_stream(stream::iter(vec![marker(), event(1)]).chain(rx1).boxed(), store.clone(), "k".into());
        let mut b = snapshot_stream(stream::iter(vec![marker()]).chain(rx2).boxed(), store.clone(), "k".into());
        a.next().await;
        a.next().await;
        b.next().await;
        assert_eq!(store.len(), 1);
        drop(a);
        assert_eq!(store.len(), 1, "b ainda assina");
        drop(b);
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn fim_do_stream_libera_a_entrada_mesmo_sem_descartar_o_cliente() {
        let store = Arc::<Store<graphql::Response>>::default();
        // Item final (subscribed: false), como o Router manda quando o upstream termina.
        let mut a = snapshot_stream(upstream(vec![marker(), event(1), end()]), store.clone(), "k".into());
        a.next().await;
        a.next().await;
        assert_eq!(store.len(), 1);
        a.next().await;
        assert_eq!(store.len(), 0, "liberado no item final, com o stream ainda vivo");
        // Fim do stream sem item final.
        let mut b = snapshot_stream(upstream(vec![marker(), event(2)]), store.clone(), "k".into());
        b.next().await;
        b.next().await;
        assert!(b.next().await.is_none());
        assert_eq!(store.len(), 0, "liberado no fim do stream");
        let frames: Vec<_> = snapshot_stream(upstream(vec![marker()]), store.clone(), "k".into()).collect().await;
        assert!(frames.iter().all(|f| !is_snapshot(f)), "quem chega depois não recebe frame do upstream encerrado");
    }

    #[tokio::test]
    async fn respostas_que_nao_sao_subscription_aberta_passam_intactas() {
        let store = Arc::<Store<graphql::Response>>::default();
        let rejected = graphql::Response::builder()
            .error(graphql::Error::builder().message("bad").extension_code("X").build())
            .subscribed(false)
            .build();
        let frames: Vec<_> = snapshot_stream(upstream(vec![rejected.clone()]), store.clone(), "k".into()).collect().await;
        assert_eq!(frames, vec![rejected]);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn chave_inclui_variaveis_e_identidade() {
        let request = |token: &str, match_id: &str| {
            http::Request::builder()
                .header("authorization", token)
                .header("x-trace", "ignorado")
                .body(
                    graphql::Request::builder()
                        .query("subscription Scoreboard($matchId: ID!) { scoreboardUpdates(matchId: $matchId) { version } }")
                        .operation_name("Scoreboard")
                        .variable("matchId", match_id)
                        .build(),
                )
                .unwrap()
        };
        let headers = vec!["authorization".to_string()];
        let key = |token, id| snapshot_key(&request(token, id), &headers);
        assert_eq!(key("a", "m1"), key("a", "m1"));
        assert_ne!(key("a", "m1"), key("b", "m1"));
        assert_ne!(key("a", "m1"), key("a", "m2"));
        assert_eq!(snapshot_key(&request("a", "m1"), &[]), snapshot_key(&request("b", "m1"), &[]));
    }
}
