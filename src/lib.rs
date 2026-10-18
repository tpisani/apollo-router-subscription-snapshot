//! `subscription_snapshot`: keeps the latest event of each deduplicated subscription
//! (the snapshot) and replays it to clients that join an upstream that is already
//! open, right after the start marker, tagged with `extensions.snapshot: true`.
//!
//! The stream that leaves the supergraph service for a subscription is:
//! 1. a marker (`subscribed == Some(true)`, no data), which the router service
//!    consumes to open the multipart response and never sends to the client;
//! 2. the events (`subscribed == Some(true)`, with data);
//! 3. a final empty item (`subscribed == Some(false)`), which closes the multipart
//!    response.
//!
//! The snapshot is therefore inserted after item 1: inserted before it, it would
//! become the marker and be lost.
//!
//! A subscriber leaves the `Store` when its stream ends or when the client is
//! dropped, whichever comes first.
//!
//! This is only correct because every frame carries the whole state and a version:
//! the client ignores a snapshot that is older than a frame already in flight.

use std::collections::HashMap;
use std::sync::Arc;

use apollo_router::Context;
use apollo_router::graphql;
use apollo_router::layers::ServiceBuilderExt;
use apollo_router::plugin::Plugin;
use apollo_router::plugin::PluginInit;
use apollo_router::register_plugin;
use apollo_router::services::supergraph;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Deserialize;
use tower::BoxError;
use tower::ServiceBuilder;
use tower::ServiceExt;

/// Context key where the Router stores the operation kind (`query`, `mutation` or
/// `subscription`). The Router does not export a constant for it.
const OPERATION_KIND: &str = "apollo::supergraph::operation_kind";

/// Response extension added to replayed snapshots, so that tests and clients can
/// tell them apart from live events.
const SNAPSHOT_EXTENSION: &str = "snapshot";

/// Configuration of the `subscription_snapshot` plugin.
#[derive(Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
struct Config {
    /// Enables the snapshot. When disabled, the plugin leaves responses untouched.
    enabled: bool,
    /// Request headers that are part of the snapshot key (the same ones the Router
    /// deduplication does not ignore, e.g. `authorization`). Different identities
    /// never share a snapshot.
    identity_headers: Vec<String>,
}

struct Entry<T> {
    latest: Option<T>,
    subscribers: usize,
}

/// Latest frame per key, kept while at least one subscriber holds that key.
struct Store<T> {
    entries: Mutex<HashMap<String, Entry<T>>>,
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
        }
    }
}

impl<T: Clone> Store<T> {
    /// Registers a subscriber for `key` and returns the retained frame, if any.
    fn join(self: &Arc<Self>, key: String) -> (Option<T>, Subscriber<T>) {
        let latest = {
            let mut entries = self.entries.lock();
            let entry = entries.entry(key.clone()).or_insert(Entry {
                latest: None,
                subscribers: 0,
            });
            entry.subscribers += 1;
            entry.latest.clone()
        };
        let subscriber = Subscriber {
            store: Arc::clone(self),
            key,
        };
        (latest, subscriber)
    }

    fn retain(&self, key: &str, frame: &T) {
        if let Some(entry) = self.entries.lock().get_mut(key) {
            entry.latest = Some(frame.clone());
        }
    }
}

impl<T> Store<T> {
    fn leave(&self, key: &str) {
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.get_mut(key) {
            entry.subscribers -= 1;
            if entry.subscribers == 0 {
                entries.remove(key);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
impl<T: Clone> Store<T> {
    fn retained(&self, key: &str) -> Option<T> {
        self.entries
            .lock()
            .get(key)
            .and_then(|entry| entry.latest.clone())
    }
}

/// A subscriber of a [`Store`] key; it leaves the store when dropped.
#[must_use = "dropping the subscriber removes it from the store"]
struct Subscriber<T> {
    store: Arc<Store<T>>,
    key: String,
}

impl<T: Clone> Subscriber<T> {
    fn retain(&self, frame: &T) {
        self.store.retain(&self.key, frame);
    }
}

impl<T> Drop for Subscriber<T> {
    fn drop(&mut self) {
        self.store.leave(&self.key);
    }
}

/// Snapshot key: document, operation name, variables and the identity headers.
fn snapshot_key(request: &http::Request<graphql::Request>, identity_headers: &[String]) -> String {
    let body = request.body();
    let identity: Vec<(&str, &str)> = identity_headers
        .iter()
        .map(|name| {
            let value = request
                .headers()
                .get(name.as_str())
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            (name.as_str(), value)
        })
        .collect();
    serde_json::json!([body.query, body.operation_name, body.variables, identity]).to_string()
}

fn is_subscription(context: &Context) -> bool {
    context
        .get::<_, String>(OPERATION_KIND)
        .ok()
        .flatten()
        .is_some_and(|kind| kind == "subscription")
}

fn is_event(frame: &graphql::Response) -> bool {
    frame.subscribed == Some(true) && frame.data.is_some() && frame.errors.is_empty()
}

/// Inserts the retained frame after the marker and retains every new event.
/// Streams that do not start with the marker of an open subscription pass through
/// untouched.
fn snapshot_stream(
    responses: graphql::ResponseStream,
    store: Arc<Store<graphql::Response>>,
    key: String,
) -> graphql::ResponseStream {
    responses
        .into_future()
        .map(move |(first, rest)| {
            let Some(first) = first else {
                return stream::empty().boxed();
            };
            if first.subscribed != Some(true) || !first.errors.is_empty() {
                return stream::once(async { first }).chain(rest).boxed();
            }
            let (retained, subscriber) = store.join(key);
            let snapshot = retained.map(|mut frame| {
                tracing::debug!("replaying the subscription snapshot to a late joiner");
                frame.extensions.insert(SNAPSHOT_EXTENSION, true.into());
                frame
            });
            // The subscriber leaves the store when the stream ends (final item with
            // `subscribed: false`, or end of the stream), not only when the client is
            // dropped: a client that arrives after the upstream ended must not get a
            // frame retained from it.
            let events = stream::unfold(
                (rest, Some(subscriber)),
                |(mut rest, mut subscriber)| async move {
                    let frame = rest.next().await?;
                    if frame.subscribed == Some(false) {
                        subscriber = None;
                    } else if let Some(subscriber) = &subscriber
                        && is_event(&frame)
                    {
                        subscriber.retain(&frame);
                    }
                    Some((frame, (rest, subscriber)))
                },
            );
            stream::once(async { first })
                .chain(stream::iter(snapshot))
                .chain(events)
                .boxed()
        })
        .flatten_stream()
        .boxed()
}

/// Replays the latest event of a deduplicated subscription to the clients that join
/// it later.
struct SubscriptionSnapshot {
    config: Config,
    store: Arc<Store<graphql::Response>>,
}

#[async_trait::async_trait]
impl Plugin for SubscriptionSnapshot {
    type Config = Config;

    async fn new(init: PluginInit<Self::Config>) -> Result<Self, BoxError> {
        Ok(Self {
            config: init.config,
            store: Arc::default(),
        })
    }

    fn supergraph_service(&self, service: supergraph::BoxService) -> supergraph::BoxService {
        if !self.config.enabled {
            return service;
        }
        let identity_headers = self.config.identity_headers.clone();
        let store = Arc::clone(&self.store);
        ServiceBuilder::new()
            .map_future_with_request_data(
                move |request: &supergraph::Request| {
                    snapshot_key(&request.supergraph_request, &identity_headers)
                },
                move |key: String, response| {
                    let store = Arc::clone(&store);
                    async move {
                        let response: supergraph::Response = response.await?;
                        if !is_subscription(&response.context) {
                            return Ok(response);
                        }
                        Ok(response.map(move |responses| snapshot_stream(responses, store, key)))
                    }
                },
            )
            .service(service)
            .boxed()
    }
}

// The group name namespaces the plugin, so its key in the `plugins:` section of the
// Router configuration is `globo.subscription_snapshot`.
register_plugin!("globo", "subscription_snapshot", SubscriptionSnapshot);

#[cfg(test)]
mod tests {
    use serde_json_bytes::json;
    use tower::Service;

    use super::*;

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
        frame
            .data
            .as_ref()?
            .get("scoreboardUpdates")?
            .get("version")?
            .as_i64()
    }

    fn is_snapshot(frame: &graphql::Response) -> bool {
        frame.extensions.get(SNAPSHOT_EXTENSION) == Some(&true.into())
    }

    fn upstream(frames: Vec<graphql::Response>) -> graphql::ResponseStream {
        stream::iter(frames).boxed()
    }

    /// An upstream that yields `frames` and then stays open until `_pending` is dropped.
    fn open_upstream(
        frames: Vec<graphql::Response>,
    ) -> (
        graphql::ResponseStream,
        futures::channel::mpsc::UnboundedSender<graphql::Response>,
    ) {
        let (pending, rx) = futures::channel::mpsc::unbounded();
        (stream::iter(frames).chain(rx).boxed(), pending)
    }

    #[tokio::test]
    async fn first_subscriber_gets_no_snapshot_and_feeds_the_store() {
        let store = Arc::<Store<graphql::Response>>::default();
        let mut responses = snapshot_stream(
            upstream(vec![marker(), event(1), event(2)]),
            store.clone(),
            "k".into(),
        );
        let first = responses.next().await.unwrap();
        assert_eq!(first.subscribed, Some(true));
        assert!(first.data.is_none(), "the marker passes through untouched");
        assert_eq!(version(&responses.next().await.unwrap()), Some(1));
        assert_eq!(version(&responses.next().await.unwrap()), Some(2));
        assert_eq!(store.retained("k").as_ref().and_then(version), Some(2));
    }

    #[tokio::test]
    async fn late_joiner_gets_the_snapshot_right_after_the_marker() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (upstream_a, _pending) = open_upstream(vec![marker(), event(7)]);
        let mut a = snapshot_stream(upstream_a, store.clone(), "k".into());
        a.next().await;
        a.next().await; // `a` saw version 7

        let b = snapshot_stream(
            upstream(vec![marker(), event(8)]),
            store.clone(),
            "k".into(),
        );
        let frames: Vec<_> = b.collect().await;
        assert!(
            frames[0].data.is_none() && frames[0].subscribed == Some(true),
            "the marker comes first"
        );
        assert!(is_snapshot(&frames[1]));
        assert_eq!(version(&frames[1]), Some(7));
        assert_eq!(
            frames[1].subscribed,
            Some(true),
            "the snapshot keeps the multipart response open"
        );
        assert_eq!(version(&frames[2]), Some(8));
        assert!(!is_snapshot(&frames[2]));
    }

    #[tokio::test]
    async fn different_keys_do_not_share_snapshots() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (upstream_a, _pending) = open_upstream(vec![marker(), event(3)]);
        let mut a = snapshot_stream(upstream_a, store.clone(), "token-a".into());
        a.next().await;
        a.next().await;
        let frames: Vec<_> =
            snapshot_stream(upstream(vec![marker()]), store.clone(), "token-b".into())
                .collect()
                .await;
        assert_eq!(frames.len(), 1);
        assert!(frames.iter().all(|frame| !is_snapshot(frame)));
    }

    #[tokio::test]
    async fn errors_are_not_retained_and_the_final_item_releases_the_entry() {
        let store = Arc::<Store<graphql::Response>>::default();
        let error = graphql::Response::builder()
            .data(json!({ "scoreboardUpdates": null }))
            .error(
                graphql::Error::builder()
                    .message("boom")
                    .extension_code("X")
                    .build(),
            )
            .subscribed(true)
            .build();
        let (upstream_a, _pending) = open_upstream(vec![marker(), event(1), error, end()]);
        let mut a = snapshot_stream(upstream_a, store.clone(), "k".into());
        for _ in 0..3 {
            a.next().await;
        }
        assert_eq!(
            store.retained("k").as_ref().and_then(version),
            Some(1),
            "the error does not replace the snapshot"
        );
        a.next().await;
        assert_eq!(store.len(), 0, "the final item releases the entry");
    }

    #[tokio::test]
    async fn the_snapshot_is_dropped_with_the_last_subscriber() {
        let store = Arc::<Store<graphql::Response>>::default();
        let (upstream_a, _pending_a) = open_upstream(vec![marker(), event(1)]);
        let (upstream_b, _pending_b) = open_upstream(vec![marker()]);
        let mut a = snapshot_stream(upstream_a, store.clone(), "k".into());
        let mut b = snapshot_stream(upstream_b, store.clone(), "k".into());
        a.next().await;
        a.next().await;
        b.next().await;
        assert_eq!(store.len(), 1);
        drop(a);
        assert_eq!(store.len(), 1, "`b` is still subscribed");
        drop(b);
        assert_eq!(store.len(), 0);
    }

    #[tokio::test]
    async fn the_end_of_the_stream_releases_the_entry_before_the_client_is_dropped() {
        let store = Arc::<Store<graphql::Response>>::default();
        // Final item (`subscribed: false`), as the Router sends when the upstream ends.
        let mut a = snapshot_stream(
            upstream(vec![marker(), event(1), end()]),
            store.clone(),
            "k".into(),
        );
        a.next().await;
        a.next().await;
        assert_eq!(store.len(), 1);
        a.next().await;
        assert_eq!(
            store.len(),
            0,
            "released on the final item, with the stream still alive"
        );

        // End of the stream without a final item.
        let mut b = snapshot_stream(
            upstream(vec![marker(), event(2)]),
            store.clone(),
            "k".into(),
        );
        b.next().await;
        b.next().await;
        assert!(b.next().await.is_none());
        assert_eq!(store.len(), 0, "released at the end of the stream");

        let frames: Vec<_> = snapshot_stream(upstream(vec![marker()]), store.clone(), "k".into())
            .collect()
            .await;
        assert!(
            frames.iter().all(|frame| !is_snapshot(frame)),
            "a client that arrives later gets no frame from the upstream that ended"
        );
    }

    #[tokio::test]
    async fn responses_that_are_not_an_open_subscription_pass_through() {
        let store = Arc::<Store<graphql::Response>>::default();
        let rejected = graphql::Response::builder()
            .error(
                graphql::Error::builder()
                    .message("bad")
                    .extension_code("X")
                    .build(),
            )
            .subscribed(false)
            .build();
        let frames: Vec<_> =
            snapshot_stream(upstream(vec![rejected.clone()]), store.clone(), "k".into())
                .collect()
                .await;
        assert_eq!(frames, vec![rejected]);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn the_key_includes_variables_and_identity() {
        let request = |token: &str, match_id: &str| {
            http::Request::builder()
                .header("authorization", token)
                .header("x-trace", "ignored")
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
        let key = |token, match_id| snapshot_key(&request(token, match_id), &headers);
        assert_eq!(key("a", "m1"), key("a", "m1"));
        assert_ne!(key("a", "m1"), key("b", "m1"));
        assert_ne!(key("a", "m1"), key("a", "m2"));
        assert_eq!(
            snapshot_key(&request("a", "m1"), &[]),
            snapshot_key(&request("b", "m1"), &[])
        );
    }

    // The tests below go through the `supergraph_service` hook, as the Router does.

    fn plugin(enabled: bool) -> SubscriptionSnapshot {
        SubscriptionSnapshot {
            config: Config {
                enabled,
                identity_headers: vec!["authorization".to_string()],
            },
            store: Arc::default(),
        }
    }

    fn scoreboard_request() -> supergraph::Request {
        supergraph::Request::fake_builder()
            .query("subscription Scoreboard { scoreboardUpdates(matchId: \"m1\") { version } }")
            .operation_name("Scoreboard")
            .header("authorization", "Bearer a")
            .build()
            .unwrap()
    }

    fn supergraph_response(
        operation_kind: &str,
        responses: graphql::ResponseStream,
    ) -> supergraph::Response {
        let context = Context::new();
        context
            .insert(OPERATION_KIND, operation_kind.to_string())
            .unwrap();
        supergraph::Response::fake_stream_builder()
            .context(context)
            .build()
            .unwrap()
            .map(|_| responses)
    }

    /// Sends one request through `service` and answers it with `upstream`.
    async fn call(
        service: &mut supergraph::BoxService,
        handle: &mut tower_test::mock::Handle<supergraph::Request, supergraph::Response>,
        operation_kind: &str,
        upstream: graphql::ResponseStream,
    ) -> graphql::ResponseStream {
        let response = service.ready().await.unwrap().call(scoreboard_request());
        let (_request, send_response) = handle.next_request().await.unwrap();
        send_response.send_response(supergraph_response(operation_kind, upstream));
        response.await.unwrap().response.into_body()
    }

    #[tokio::test]
    async fn the_plugin_is_registered() {
        let config = serde_json::json!({
            "plugins": {
                "globo.subscription_snapshot": {
                    "enabled": true,
                    "identity_headers": ["authorization"]
                }
            }
        });
        apollo_router::TestHarness::builder()
            .configuration_json(config)
            .unwrap()
            .build_router()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_supergraph_service_replays_the_snapshot_to_late_joiners() {
        let plugin = plugin(true);
        let (mock, mut handle) = tower_test::mock::pair();
        let mut service = plugin.supergraph_service(mock.boxed());

        let (upstream_a, _pending) = open_upstream(vec![marker(), event(7)]);
        let mut a = call(&mut service, &mut handle, "subscription", upstream_a).await;
        a.next().await;
        a.next().await;

        let b = call(
            &mut service,
            &mut handle,
            "subscription",
            upstream(vec![marker(), event(8)]),
        )
        .await;
        let frames: Vec<_> = b.collect().await;
        assert_eq!(frames.len(), 3);
        assert!(is_snapshot(&frames[1]));
        assert_eq!(version(&frames[1]), Some(7));
        assert_eq!(version(&frames[2]), Some(8));
    }

    #[tokio::test]
    async fn the_supergraph_service_ignores_other_operations() {
        let plugin = plugin(true);
        let (mock, mut handle) = tower_test::mock::pair();
        let mut service = plugin.supergraph_service(mock.boxed());

        let frames: Vec<_> = call(&mut service, &mut handle, "query", upstream(vec![event(1)]))
            .await
            .collect()
            .await;
        assert_eq!(frames, vec![event(1)]);
        assert_eq!(plugin.store.len(), 0);
    }

    #[tokio::test]
    async fn the_supergraph_service_is_untouched_when_disabled() {
        let plugin = plugin(false);
        let (mock, mut handle) = tower_test::mock::pair();
        let mut service = plugin.supergraph_service(mock.boxed());

        let (upstream_a, _pending) = open_upstream(vec![marker(), event(7)]);
        let mut a = call(&mut service, &mut handle, "subscription", upstream_a).await;
        a.next().await;
        a.next().await;
        assert_eq!(plugin.store.len(), 0);

        let frames: Vec<_> = call(
            &mut service,
            &mut handle,
            "subscription",
            upstream(vec![marker(), event(8)]),
        )
        .await
        .collect()
        .await;
        assert!(frames.iter().all(|frame| !is_snapshot(frame)));
    }
}
