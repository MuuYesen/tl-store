// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Explicit account-scoped session ownership shared by native data and execution clients.

use super::{
    OpenApiClient, OpenApiConfig, OpenApiCredentials, OpenApiEvents,
    oauth::{OAuthAdmission, OAuthConfig, OAuthGrant, required_env},
};
use anyhow::{Context, Result};
use std::{
    future::Future,
    sync::{Arc, Weak},
};
use tokio::sync::Mutex;

struct Epoch {
    client: OpenApiClient,
    exclusive: bool,
    number: u64,
    oauth_generation: u64,
    oauth_admission: Option<OAuthAdmission>,
}
impl Drop for Epoch {
    fn drop(&mut self) {
        self.client.request_close();
    }
}
struct SessionState {
    epoch: Weak<Epoch>,
    sequence: u64,
}
/// Immutable account identity and transport settings, explicitly shared by its consumers.
///
/// The session retains only a weak epoch reference. The final lease ends its socket,
/// while a subsequent acquire authenticates a fresh epoch before returning.
pub struct OpenApiAccountSession {
    config: OpenApiConfig,
    exclusive: bool,
    state: Mutex<SessionState>,
    oauth_config: Option<OAuthConfig>,
    explicit_oauth: Option<(String, Option<(String, String)>)>,
    oauth: std::sync::Mutex<Option<Arc<OAuthGrant>>>,
    oauth_watch: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl std::fmt::Debug for OpenApiAccountSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenApiAccountSession")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl OpenApiAccountSession {
    /// Creates an account scope without network or token-file I/O.
    /// Managed environment bindings are checked now; secrets are loaded lazily.
    ///
    /// # Errors
    /// Returns an error for invalid identity or transport bounds.
    pub fn new(config: OpenApiConfig) -> Result<Self> {
        let oauth_config = OAuthConfig::from_env(&config)?;
        Self::build(config, oauth_config, None)
    }
    /// Retains instance credentials without environment, file, network or task I/O.
    pub fn new_with_credentials(config: OpenApiConfig, oauth: OAuthConfig,
                                secret: String, seed: Option<(String, String)>) -> Result<Self> {
        anyhow::ensure!(oauth.matches_account(&config), "OAuth grant/account identity mismatch");
        anyhow::ensure!(!secret.is_empty(), "Explicit OAuth client secret required");
        if let Some((access, refresh)) = &seed {
            anyhow::ensure!(!access.is_empty() && !refresh.is_empty(), "Explicit OAuth seed pair required");
        }
        let mut session = Self::build(config, Some(oauth), None)?;
        session.explicit_oauth = Some((secret, seed));
        Ok(session)
    }
    /// Injects one explicit managed grant without reading process-wide token settings.
    pub fn new_with_oauth(config: OpenApiConfig, grant: Arc<OAuthGrant>) -> Result<Self> {
        anyhow::ensure!(
            grant.config().matches_account(&config),
            "OAuth grant/account identity mismatch"
        );
        Self::build(config, None, Some(grant))
    }
    /// Creates an independent socket owner for the same account and OAuth grant.
    ///
    /// The peer permits one lease, keeps its own socket and event queue, and
    /// reuses the existing grant owner. Managed grants require an async runtime.
    ///
    /// # Errors
    /// Returns an error if the shared grant cannot be initialized or is invalid.
    pub fn isolated_peer(&self) -> Result<Self> {
        self.isolated_peer_with_capacity(self.config.event_capacity)
    }
    /// Creates an isolated peer with its own explicitly bounded event queue.
    ///
    /// # Errors
    /// Returns an error for invalid capacity or an unavailable shared grant.
    pub fn isolated_peer_with_capacity(&self, event_capacity: usize) -> Result<Self> {
        self.isolated_peer_with_limits(event_capacity, self.config.max_unsolicited_event_bytes)
    }
    pub(super) fn isolated_peer_with_limits(
        &self,
        event_capacity: usize,
        max_unsolicited_event_bytes: usize,
    ) -> Result<Self> {
        anyhow::ensure!(
            (1..=4096).contains(&event_capacity),
            "Invalid isolated queue capacity"
        );
        let mut config = self.config.clone();
        config.event_capacity = event_capacity;
        config.max_unsolicited_event_bytes = max_unsolicited_event_bytes;
        let mut peer = Self::build(config, None, self.managed_grant()?)?;
        peer.exclusive = true;
        Ok(peer)
    }
    fn build(
        config: OpenApiConfig,
        oauth_config: Option<OAuthConfig>,
        grant: Option<Arc<OAuthGrant>>,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            exclusive: false,
            state: Mutex::new(SessionState {
                epoch: Weak::new(),
                sequence: 0,
            }),
            oauth_config,
            explicit_oauth: None,
            oauth: std::sync::Mutex::new(grant),
            oauth_watch: std::sync::Mutex::new(None),
        })
    }
    fn managed_grant(&self) -> Result<Option<Arc<OAuthGrant>>> {
        let mut slot = self
            .oauth
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth session source lock unavailable"))?;
        if slot.is_none()
            && let Some(config) = &self.oauth_config
        {
            *slot = Some(if let Some((secret, seed)) = &self.explicit_oauth {
                if let Some((access, refresh)) = seed {
                    OAuthGrant::start_seeded(config.clone(), secret.clone(), access.clone(), refresh.clone())?
                } else {
                    OAuthGrant::start(config.clone(), secret.clone())?
                }
            } else {
                OAuthGrant::start(config.clone(), required_env("CTRADER_OPENAPI_CLIENT_SECRET")?)?
            });
        }
        Ok(slot.clone())
    }
    fn current_generation(&self) -> Result<u64> {
        let source = self
            .oauth
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth session source lock unavailable"))?
            .clone();
        source.map_or(Ok(0), |grant| {
            grant.credentials().map(|(generation, _)| generation)
        })
    }
    fn watch_grant(self: &Arc<Self>, grant: &OAuthGrant) -> Result<()> {
        let mut watcher = self
            .oauth_watch
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth session watcher lock unavailable"))?;
        if watcher.is_some() {
            return Ok(());
        }
        let mut changes = grant.subscribe();
        let weak = Arc::downgrade(self);
        *watcher = Some(tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                let status = *changes.borrow_and_update();
                let Some(session) = weak.upgrade() else {
                    break;
                };
                let state = session.state.lock().await;
                if let Some(epoch) = state.epoch.upgrade()
                    && (!status.ready || status.generation != epoch.oauth_generation)
                {
                    epoch.client.request_close();
                }
            }
        }));
        Ok(())
    }
    pub async fn invalidate(&self) {
        if let Some(epoch) = self.state.lock().await.epoch.upgrade() {
            epoch.client.request_close();
        }
    }
    pub fn config(&self) -> &OpenApiConfig {
        &self.config
    }
    pub(super) async fn owns_lease(&self, lease: &OpenApiLease) -> bool {
        self.state
            .lock()
            .await
            .epoch
            .upgrade()
            .is_some_and(|epoch| Arc::ptr_eq(&epoch, &lease.epoch))
    }
    /// Registers this consumer before it can request metadata or reconciliation snapshots.
    ///
    /// # Errors
    /// Returns an error if authentication, subscription admission, or transport fails.
    pub async fn acquire(self: &Arc<Self>) -> Result<OpenApiLease> {
        if let Some(grant) = self.managed_grant()? {
            self.watch_grant(&grant)?;
            let generation = grant.ready_credentials().await?.0;
            Box::pin(self.acquire_connected_at(
                async {
                    let (_, credentials) = grant.credentials()?;
                    OpenApiClient::connect(self.config.clone(), credentials).await
                },
                generation,
            ))
            .await
        } else {
            // Static access tokens remain an explicit diagnostic mode. Formal strategy
            // deployments require TOKEN_STATE/TOKEN_ROLE and cannot silently fall back.
            Box::pin(self.acquire_connected_at(
                async {
                    OpenApiClient::connect(self.config.clone(), OpenApiCredentials::from_env()?)
                        .await
                },
                0,
            ))
            .await
        }
    }
    pub(crate) async fn acquire_connected_at(
        &self,
        connection: impl Future<Output = Result<OpenApiClient>>,
        generation: u64,
    ) -> Result<OpenApiLease> {
        // Authentication and epoch replacement are single-flight. A failed old epoch
        // cannot be returned to a new consumer, even while an old lease still exists.
        let mut state = self.state.lock().await;
        if !self
            .current_generation()
            .is_ok_and(|current| current == generation)
        {
            if let Some(epoch) = state.epoch.upgrade() {
                epoch.client.request_close();
            }
            anyhow::bail!("OAuth generation changed or grant is unavailable before authentication");
        }
        if let Some(epoch) = state.epoch.upgrade() {
            if epoch.oauth_generation == generation && epoch.client.is_connected().await {
                anyhow::ensure!(!self.exclusive, "Isolated Open API session already leased");
                let events = epoch.client.subscribe_events()?;
                return Ok(OpenApiLease {
                    subscription_id: events.subscription_id,
                    epoch,
                    events: Some(events),
                });
            }
            epoch.client.request_close();
        }
        let client = connection.await?;
        if !self
            .current_generation()
            .is_ok_and(|current| current == generation)
        {
            client.request_close();
            anyhow::bail!("OAuth generation changed during authentication");
        }
        let events = client.take_events()?;
        anyhow::ensure!(
            client.is_connected().await,
            "Open API failed during authentication"
        );
        state.sequence = state
            .sequence
            .checked_add(1)
            .context("Open API session epoch exhausted")?;
        let oauth_admission = self
            .oauth
            .lock()
            .map_err(|_| anyhow::anyhow!("OAuth session source lock unavailable"))?
            .as_ref()
            .map(|grant| grant.admission(generation));
        let epoch = Arc::new(Epoch {
            client,
            exclusive: self.exclusive,
            number: state.sequence,
            oauth_generation: generation,
            oauth_admission,
        });
        state.epoch = Arc::downgrade(&epoch);
        Ok(OpenApiLease {
            subscription_id: events.subscription_id,
            epoch,
            events: Some(events),
        })
    }
}
impl Drop for OpenApiAccountSession {
    fn drop(&mut self) {
        if let Ok(watcher) = self.oauth_watch.get_mut()
            && let Some(watcher) = watcher.take()
        {
            watcher.abort();
        }
    }
}
/// Exact epoch/generation admission checked before the first native FIX byte.
/// Holding this signal does not keep a client or socket alive.
#[derive(Clone)]
pub struct OpenApiAdmission {
    activity: Arc<std::sync::atomic::AtomicBool>,
    oauth: Option<OAuthAdmission>,
    revision: Option<(Arc<std::sync::atomic::AtomicU64>, u64)>,
}
impl std::fmt::Debug for OpenApiAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpenApiAdmission([epoch authorization fence])")
    }
}
impl OpenApiAdmission {
    pub fn new(
        activity: Arc<std::sync::atomic::AtomicBool>,
        oauth: Option<OAuthAdmission>,
    ) -> Self {
        Self {
            activity,
            oauth,
            revision: None,
        }
    }
    /// Fences a queued operation if its observed account revision changes before first write.
    #[must_use]
    pub fn with_revision(
        mut self,
        revision: Arc<std::sync::atomic::AtomicU64>,
        expected: u64,
    ) -> Self {
        self.revision = Some((revision, expected));
        self
    }
    pub fn check(&self) -> Result<()> {
        anyhow::ensure!(
            self.activity.load(std::sync::atomic::Ordering::Acquire),
            "Open API epoch is closed"
        );
        if let Some(oauth) = &self.oauth {
            oauth.check()?;
        }
        if let Some((revision, expected)) = &self.revision {
            anyhow::ensure!(
                revision.load(std::sync::atomic::Ordering::Acquire) == *expected,
                "Account revision changed before dispatch"
            );
        }
        Ok(())
    }
}
/// One consumer's ownership of an authenticated epoch and independent event queue.
pub struct OpenApiLease {
    subscription_id: u64,
    epoch: Arc<Epoch>,
    events: Option<OpenApiEvents>,
}
impl Drop for OpenApiLease {
    fn drop(&mut self) {
        self.epoch.client.unsubscribe_events(self.subscription_id);
    }
}
impl OpenApiLease {
    pub fn admission_guard(&self) -> OpenApiAdmission {
        OpenApiAdmission::new(
            self.epoch.client.activity_fence(),
            self.epoch.oauth_admission.clone(),
        )
    }
    pub fn client(&self) -> &OpenApiClient {
        &self.epoch.client
    }
    pub fn epoch(&self) -> u64 {
        self.epoch.number
    }
    /// Returns whether this lease is the sole consumer of an isolated socket.
    pub fn is_isolated(&self) -> bool {
        self.epoch.exclusive
    }
    /// Transfers this lease's independent bounded event receiver.
    ///
    /// # Errors
    /// Returns an error if the receiver was already taken.
    pub fn take_events(&mut self) -> Result<OpenApiEvents> {
        self.events
            .take()
            .context("Open API lease events already taken")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::{OpenApiEnvironment, OpenApiEvent, ReadOnlyRequest};
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use std::time::Duration;
    use tokio::{net::TcpListener, sync::oneshot, time::timeout};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    async fn peer() -> (String, oneshot::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (closed_tx, closed_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let req: Value = serde_json::from_str(&text).unwrap();
                let kind = req["payloadType"].as_u64().unwrap();
                let (response, payload) = match kind {
                    51 => continue,
                    2100 => (2101, json!({})),
                    2149 => (
                        2150,
                        json!({"accessToken":req["payload"]["accessToken"],"permissionScope":0,"ctidTraderAccount":[{"ctidTraderAccountId":"42","traderLogin":"123","isLive":false}]}),
                    ),
                    2102 => (2103, json!({"ctidTraderAccountId":"42"})),
                    2114 => (
                        2115,
                        json!({"ctidTraderAccountId":"42","symbol":[
                        {"symbolId":"7","symbolName":"BTCUSD"},
                        {"symbolId":"8","symbolName":"XAUUSD"},
                        {"symbolId":"9","symbolName":"TEST"}]}),
                    ),
                    2127 => (2128, json!({"ctidTraderAccountId":"42"})),
                    2156 => (2157, json!({"ctidTraderAccountId":"42"})),
                    2121 => (
                        2122,
                        json!({"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42","traderLogin":"123","depositAssetId":"1","balance":"100","moneyDigits":2}}),
                    ),
                    other => panic!("unexpected request {other}"),
                };
                if req["payload"]["orphan"] == true {
                    // Deliberately send a response with no matching in-flight request.
                    // Do not also send the valid reply: this fixture must exercise the
                    // reader's unmatched-response path rather than normal completion.
                    let _ = socket
                        .send(Message::Text(
                            json!({
                                "clientMsgId":"unmatched-request", "payloadType":response,
                                "payload":payload,
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                    continue;
                }
                if socket.send(Message::Text(json!({"clientMsgId":req["clientMsgId"],"payloadType":response,"payload":payload}).to_string().into())).await.is_err(){break;}
                if kind == 2127
                    && socket
                        .send(Message::Text(
                            json!({"payloadType":2131,"payload":{
                                "ctidTraderAccountId":"42","symbolId":req["payload"]["symbolId"][0],
                                "bid":"123456","timestamp":"1793000000000"
                            }})
                            .to_string()
                            .into(),
                        ))
                        .await
                        .is_err()
                {
                    break;
                }
                if kind == 2156 {
                    let mut depth = json!({"payloadType":2155,"payload":{
                        "ctidTraderAccountId":"42","symbolId":"7",
                        "newQuotes":[{"id":"1","size":"100","bid":"123456"},
                                     {"id":"2","size":"50","ask":"123500"}]
                    }});
                    if req["payload"]["symbolId"][0] == 9 {
                        depth["payload"]["padding"] = json!("x".repeat(4096));
                    }
                    if socket
                        .send(Message::Text(depth.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                if kind == 2121
                    && socket
                        .send(Message::Text(
                            json!({"clientMsgId":req["clientMsgId"],"payloadType":2123,"payload":{"ctidTraderAccountId":"42","marker":req["payload"]["marker"]}})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            let _ = closed_tx.send(());
        });
        (endpoint, closed_rx)
    }
    fn config(capacity: usize) -> OpenApiConfig {
        let mut config = OpenApiConfig::demo(123);
        config.environment = OpenApiEnvironment::Demo;
        config.event_capacity = capacity;
        config
    }
    fn credentials() -> OpenApiCredentials {
        OpenApiCredentials {
            client_id: "id".into(),
            client_secret: "secret".into(),
            access_token: "token".into(),
        }
    }
    async fn acquire(session: &Arc<OpenApiAccountSession>, endpoint: &str) -> OpenApiLease {
        Box::pin(session.acquire_connected_at(
            OpenApiClient::connect_to(endpoint, session.config().clone(), credentials()),
            0,
        ))
        .await
        .unwrap()
    }
    async fn marked(events: &mut OpenApiEvents) -> OpenApiEvent {
        timeout(Duration::from_secs(2), async {
            loop {
                let event = events.next().await.unwrap();
                if event.payload["marker"] == "fanout" {
                    return event;
                }
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test]
    async fn concurrent_acquire_authenticates_once_and_fans_out_to_both_leases() {
        let (endpoint, mut closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let (mut first, mut second) =
            tokio::join!(acquire(&session, &endpoint), acquire(&session, &endpoint));
        assert_eq!(first.epoch(), second.epoch());
        let mut first_events = first.take_events().unwrap();
        let mut second_events = second.take_events().unwrap();
        // The bootstrap receiver also contains the event sent during authentication.
        first_events.next().await.unwrap();
        first
            .client()
            .request(ReadOnlyRequest::Trader, json!({"marker":"fanout"}))
            .await
            .unwrap();
        let a = marked(&mut first_events).await;
        let b = marked(&mut second_events).await;
        assert_eq!(a.payload, b.payload);
        drop(first);
        assert!(second.client().is_connected().await);
        assert!(
            timeout(Duration::from_millis(20), &mut closed)
                .await
                .is_err()
        );
        let escaped = second.client().clone();
        // Client teardown may originate outside a Tokio runtime context.
        std::thread::spawn(move || drop(second)).join().unwrap();
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        assert!(!escaped.is_connected().await);
    }

    #[tokio::test]
    async fn isolated_peer_uses_a_distinct_socket_and_preserves_account_scope() {
        let (first_endpoint, first_closed) = peer().await;
        let (second_endpoint, second_closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer().unwrap());
        assert_eq!(primary.config(), isolated.config());
        let first = acquire(&primary, &first_endpoint).await;
        let second = acquire(&isolated, &second_endpoint).await;
        assert!(!Arc::ptr_eq(&first.epoch, &second.epoch));
        let duplicate = Box::pin(isolated.acquire_connected_at(std::future::pending(), 0)).await;
        assert!(duplicate.is_err());
        drop(first);
        timeout(Duration::from_secs(2), first_closed)
            .await
            .unwrap()
            .unwrap();
        assert!(second.client().is_connected().await);
        second.client().account_snapshot().await.unwrap();
        drop(second);
        timeout(Duration::from_secs(2), second_closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn isolated_peer_can_bound_market_queue_without_changing_primary() {
        let primary = OpenApiAccountSession::new(config(1024)).unwrap();
        let market = primary.isolated_peer_with_capacity(8).unwrap();
        assert_eq!(primary.config().event_capacity, 1024);
        assert_eq!(market.config().event_capacity, 8);
        assert_eq!(
            primary.config().expected_trader_login,
            market.config().expected_trader_login
        );
        assert!(primary.isolated_peer_with_capacity(0).is_err());
    }

    #[tokio::test]
    async fn raw_market_stream_preserves_broker_events_without_building_depth() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        let mut stream = OpenApiMarketStream::attach(isolated, lease, 2, 10, 10, 1024, 8192)
            .await
            .unwrap();
        assert_eq!(stream.account_id(), 42);
        stream
            .resolve_symbols(&["BTCUSD".to_owned()])
            .await
            .unwrap();
        stream.subscribe(7).await.unwrap();
        let spot = timeout(Duration::from_secs(2), stream.next_raw())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(spot.payload_type, 2131);
        assert_eq!(spot.raw_payload["bid"], "123456");
        let depth = timeout(Duration::from_secs(2), stream.next_raw())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(depth.payload_type, 2155);
        assert_eq!(depth.raw_payload["newQuotes"].as_array().unwrap().len(), 2);
        assert!(depth.to_wire().get("bids").is_none());
        assert!(stream.next().await.is_err());
        drop(stream);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn isolated_market_stream_subscribes_and_reports_only_observed_facts() {
        use crate::openapi::{MarketObservation, OpenApiMarketStream};
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        let weak = Arc::downgrade(&isolated);
        let mut stream =
            OpenApiMarketStream::attach(isolated.clone(), lease, 2, 10, 10, 1024, 8192)
                .await
                .unwrap();
        drop(isolated);
        drop(primary);
        assert!(weak.upgrade().is_some());
        assert_eq!(
            stream
                .resolve_symbols(&["BTCUSD".to_owned()])
                .await
                .unwrap(),
            vec![("BTCUSD".to_owned(), 7)]
        );
        stream.subscribe(7).await.unwrap();
        let spot = timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(spot.raw_payload["bid"], "123456");
        assert_eq!(spot.to_wire()["observation"]["bid"], "123456");
        assert!(spot.to_wire()["observation"]["ask"].is_null());
        let MarketObservation::Spot(spot) = spot.observation else {
            panic!("Expected spot update");
        };
        assert_eq!(
            (spot.bid, spot.ask, spot.broker_timestamp_ms),
            (Some(123456), None, Some(1793000000000))
        );
        let observation = timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            observation.raw_payload["newQuotes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(observation.to_wire()["observation"]["bids"][0][0], "123456");
        let MarketObservation::Depth(observation) = observation.observation else {
            panic!("Expected depth update");
        };
        assert_eq!(observation.account_id, 42);
        assert_eq!(observation.symbol_id, 7);
        assert_eq!(observation.bids, vec![(123_456, 100)]);
        assert_eq!(observation.asks, vec![(123_500, 50)]);
        assert_eq!(observation.source_ts_ns, None);
        assert!(!observation.complete);
        assert!(observation.received_mono_ns > 0 && observation.received_utc_ns > 0);
        drop(stream);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn market_stream_rejects_a_shared_account_lease() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let lease = acquire(&session, &endpoint).await;
        assert!(OpenApiMarketStream::new(lease, 2, 10, 10, 1024, 8192).is_err());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn market_stream_rejects_a_lease_owned_by_another_session() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        assert!(
            OpenApiMarketStream::attach(primary, lease, 2, 10, 10, 1024, 8192)
                .await
                .is_err()
        );
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn market_stream_closes_isolated_epoch_on_unsubscribed_symbol() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        let mut stream = OpenApiMarketStream::new(lease, 1, 10, 10, 1024, 8192).unwrap();
        stream
            .resolve_symbols(&["XAUUSD".to_owned()])
            .await
            .unwrap();
        stream.subscribe(8).await.unwrap();
        assert!(stream.next().await.is_ok());
        assert!(stream.next().await.is_err());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn market_stream_rejects_unbounded_queue_and_total_book_budget() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        assert!(OpenApiMarketStream::new(lease, 2, 10, 10, 1024, 4096).is_err());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        let (endpoint, closed) = peer().await;
        let lease = acquire(&isolated, &endpoint).await;
        assert!(OpenApiMarketStream::new(lease, 1000, 1000, 10, 1024, 8192).is_err());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn market_stream_closes_epoch_on_oversized_unsolicited_event() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        let mut stream = OpenApiMarketStream::new(lease, 1, 10, 10, 1024, 8192).unwrap();
        stream.resolve_symbols(&["TEST".to_owned()]).await.unwrap();
        if stream.subscribe(9).await.is_ok() {
            let error = timeout(Duration::from_secs(2), async {
                loop {
                    if stream.next().await.is_err() {
                        break;
                    }
                }
            })
            .await;
            assert!(error.is_ok());
        }
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn market_stream_refuses_unverified_or_ambiguous_symbol_identity() {
        use crate::openapi::OpenApiMarketStream;
        let (endpoint, closed) = peer().await;
        let primary = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let isolated = Arc::new(primary.isolated_peer_with_limits(8, 1024).unwrap());
        let lease = acquire(&isolated, &endpoint).await;
        let mut stream = OpenApiMarketStream::new(lease, 2, 10, 10, 1024, 8192).unwrap();
        assert!(stream.subscribe(7).await.is_err());
        assert!(
            stream
                .resolve_symbols(&["MISSING".to_owned()])
                .await
                .is_err()
        );
        assert!(
            stream
                .resolve_symbols(&["BTCUSD".to_owned(), "BTCUSD".to_owned()])
                .await
                .is_err()
        );
        assert_eq!(
            stream
                .resolve_symbols(&["BTCUSD".to_owned()])
                .await
                .unwrap(),
            vec![("BTCUSD".to_owned(), 7)]
        );
        assert!(stream.subscribe(8).await.is_err());
        drop(stream);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn lag_fails_entire_epoch_and_reacquire_creates_fresh_subscription() {
        let (endpoint, closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(1)).unwrap());
        let mut first = acquire(&session, &endpoint).await;
        let epoch = first.epoch();
        let mut events = first.take_events().unwrap();
        // Leave authentication's event queued; another event must overflow immediately.
        let _ = first.client().account_snapshot().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        assert!(!first.client().is_connected().await);
        assert!(events.next().await.is_none());
        assert!(events.try_next().is_err());
        let (endpoint, closed) = peer().await;
        let mut second = acquire(&session, &endpoint).await;
        assert_ne!(epoch, second.epoch());
        assert!(second.client().is_connected().await);
        let mut fresh = second.take_events().unwrap();
        assert_eq!(fresh.next().await.unwrap().payload_type, 2123);
        drop(first);
        assert!(second.client().is_connected().await);
        drop(second);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn dropping_an_event_consumer_does_not_disable_the_other_lease() {
        let (endpoint, closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let mut first = acquire(&session, &endpoint).await;
        let mut second = acquire(&session, &endpoint).await;
        drop(first.take_events().unwrap());
        let mut events = second.take_events().unwrap();
        second
            .client()
            .request(ReadOnlyRequest::Trader, json!({}))
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), events.next())
                .await
                .unwrap()
                .unwrap()
                .payload_type,
            2123
        );
        assert!(second.client().is_connected().await);
        drop(first);
        drop(second);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn unmatched_response_fails_the_epoch_instead_of_being_discarded() {
        let (endpoint, closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let mut lease = acquire(&session, &endpoint).await;
        let mut events = lease.take_events().unwrap();
        let result = lease
            .client()
            .request(ReadOnlyRequest::Trader, json!({"orphan":true}))
            .await;
        assert!(result.is_err());
        assert!(!lease.client().is_connected().await);
        assert!(events.next().await.is_none());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
    #[tokio::test]
    async fn committed_oauth_generation_closes_old_epoch_and_reauthenticates_only_a_fresh_lease() {
        use crate::openapi::oauth::{OAuthConfig, OAuthGrant, OAuthRole, OAuthTokenStore};
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let directory = std::env::temp_dir().join(format!(
            "ctrader-oauth-session-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = directory.join("tokens.json");
        let binding = OAuthConfig::new(
            OAuthRole::Owner,
            path.clone(),
            "id".into(),
            "session-test".into(),
            123,
            false,
        )
        .unwrap();
        let store = OAuthTokenStore::new(binding.clone()).unwrap();
        store
            .import_initial(
                "token".into(),
                "initial-refresh".into(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                3600,
            )
            .unwrap();
        let mut follower_binding = binding;
        follower_binding.role = OAuthRole::Follower;
        let grant = OAuthGrant::start(follower_binding, "secret".into()).unwrap();
        let session =
            Arc::new(OpenApiAccountSession::new_with_oauth(config(8), grant.clone()).unwrap());
        let isolated = session.isolated_peer().unwrap();
        assert!(Arc::ptr_eq(
            &grant,
            &isolated.managed_grant().unwrap().unwrap()
        ));
        session.watch_grant(&grant).unwrap();
        let (endpoint, closed) = peer().await;
        let lease = Box::pin(session.acquire_connected_at(
            OpenApiClient::connect_to(
                &endpoint,
                session.config().clone(),
                grant.credentials().unwrap().1,
            ),
            1,
        ))
        .await
        .unwrap();
        let old_epoch = lease.epoch();
        let old_guard = lease.admission_guard();
        assert!(old_guard.check().is_ok());
        let mut changes = grant.subscribe();
        let mut record: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        record["generation"] = json!(2);
        record["access_token"] = json!("second-token");
        record["refresh_token"] = json!("second-refresh");
        let next = directory.join("next.json");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&next)
            .unwrap();
        file.write_all(&serde_json::to_vec(&record).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        std::fs::rename(next, &path).unwrap();
        timeout(Duration::from_secs(2), async {
            loop {
                changes.changed().await.unwrap();
                if changes.borrow().generation == 2 && changes.borrow().ready {
                    break;
                }
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        assert!(!lease.client().is_connected().await);
        assert!(old_guard.check().is_err());
        assert!(
            lease
                .client()
                .request(ReadOnlyRequest::Trader, json!({}))
                .await
                .is_err()
        );
        let (endpoint, closed) = peer().await;
        let fresh = Box::pin(session.acquire_connected_at(
            OpenApiClient::connect_to(
                &endpoint,
                session.config().clone(),
                grant.credentials().unwrap().1,
            ),
            2,
        ))
        .await
        .unwrap();
        assert!(fresh.epoch() > old_epoch);
        assert!(fresh.client().is_connected().await);
        let fresh_guard = fresh.admission_guard();
        assert!(fresh_guard.check().is_ok());
        drop(lease);
        drop(fresh);
        assert!(fresh_guard.check().is_err());
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        drop(session);
        drop(grant);
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test]
    async fn observer_epoch_health_never_reconnects_and_rejects_changed_revision() {
        let (endpoint, closed) = peer().await;
        let session = Arc::new(OpenApiAccountSession::new(config(8)).unwrap());
        let mut lease = acquire(&session, &endpoint).await;
        let mut events = lease.take_events().unwrap();
        // Authentication's Trader request is followed by the mock's account event.
        timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        let revision = lease.client().account_revision();
        let healthy = std::sync::atomic::AtomicBool::new(true);
        assert!(
            check_owned_epoch(&lease, &healthy, lease.epoch(), Some(revision))
                .await
                .is_ok()
        );
        assert!(
            check_owned_epoch(&lease, &healthy, lease.epoch() + 1, None)
                .await
                .is_err()
        );
        lease.client().account_snapshot().await.unwrap();
        timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        assert!(
            check_owned_epoch(&lease, &healthy, lease.epoch(), Some(revision))
                .await
                .is_err()
        );
        assert!(
            check_owned_epoch(&lease, &healthy, lease.epoch(), None)
                .await
                .is_ok()
        );
        lease.client().request_close();
        assert!(
            check_owned_epoch(&lease, &healthy, lease.epoch(), None)
                .await
                .is_err()
        );
        drop(lease);
        drop(session);
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
}

#[cfg(test)]
async fn check_owned_epoch(
    lease: &OpenApiLease,
    healthy: &std::sync::atomic::AtomicBool,
    expected: u64,
    revision: Option<u64>,
) -> Result<()> {
    anyhow::ensure!(
        healthy.load(std::sync::atomic::Ordering::Acquire) && lease.epoch() == expected,
        "Account epoch mismatch"
    );
    anyhow::ensure!(lease.client().is_connected().await, "Account disconnected");
    anyhow::ensure!(
        revision.is_none_or(|v| v == lease.client().account_revision()),
        "Account revision changed"
    );
    Ok(())
}
