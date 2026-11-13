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

//! One authenticated, continuously drained JSON WebSocket for read-only account data.

fn readonly_rejection(payload: &serde_json::Value) -> anyhow::Error {
    // Preserve only the protocol error code, never description or account/order
    // payload. Invalid/non-code content remains explicitly unavailable.
    let code = payload.get("errorCode").and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 128
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'));
    match code {
        Some(code) => anyhow::anyhow!("Open API rejected request [errorCode={code}]"),
        None => anyhow::anyhow!("Open API rejected request"),
    }
}

use super::{OpenApiConfig, OpenApiCredentials, ReadOnlyRequest};
use crate::clock::monotonic_ns as posix_monotonic_ns;
use crate::openapi::model::{BrokerAccountSnapshot, BrokerDeal};
use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Semaphore, mpsc, oneshot},
    time::{interval, timeout},
};
use tokio_rustls::rustls;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Writer = SplitSink<Socket, Message>;

const HEARTBEAT_TYPE: u32 = 51;
// A real 1,000-order history page exceeds 1 MiB; keep a bounded 8 MiB envelope
pub(super) const MAX_MESSAGE_BYTES: usize = 8 * 1_048_576;
const MAX_HISTORY_DEALS: usize = 100_000;
const MAX_EVENT_SUBSCRIBERS: usize = 16;

fn extend_history_deals(
    deals: &mut HashMap<u64, BrokerDeal>,
    rows: &[Value],
    bound: usize,
) -> Result<()> {
    for row in rows {
        let deal_id = row
            .get("dealId")
            .and_then(wire_u64)
            .filter(|id| *id > 0)
            .context("Invalid Open API deal identity")?;
        if !deals.contains_key(&deal_id) && deals.len() >= bound {
            bail!("Open API deal history exceeds memory bound");
        }
        // Decode can clone close accounting JSON, so enforce the budget first
        let deal = BrokerDeal::from_wire(row)?;
        if let Some(previous) = deals.get(&deal_id) {
            if previous != &deal {
                bail!("Open API duplicate deal disagrees");
            }
        }
        // Preserve the original last-equal-row representation without an extra clone
        deals.insert(deal_id, deal);
    }
    Ok(())
}

/// A broker event with its local receive time for later causal and latency joins.
#[derive(Clone, Debug)]
pub struct OpenApiEvent {
    pub client_message_id: Option<String>,
    pub payload_type: u32,
    pub payload: Value,
    /// Reader-owned account revision after this event was observed.
    pub account_revision: u64,
    pub received_at: Instant,
    pub received_wall_time: SystemTime,
    /// POSIX CLOCK_MONOTONIC nanoseconds, comparable with FIX receive timestamps.
    pub received_mono_ns: u64,
}

/// One bounded event consumer. Overflow fails the entire socket epoch immediately.
pub struct OpenApiEvents {
    receiver: mpsc::Receiver<OpenApiEvent>,
    inner: Weak<Inner>,
    pub(super) subscription_id: u64,
}
impl OpenApiEvents {
    pub async fn next(&mut self) -> Option<OpenApiEvent> {
        let inner = self.inner.upgrade()?;
        if !inner.active.load(Ordering::Acquire) {
            return None;
        }
        let event = self.receiver.recv().await?;
        inner.active.load(Ordering::Acquire).then_some(event)
    }
    /// Returns None only for an empty healthy queue; closure and lag are errors.
    ///
    /// # Errors
    /// Returns an error when this epoch or its receiver is unavailable.
    pub fn try_next(&mut self) -> Result<Option<OpenApiEvent>> {
        let inner = self.inner.upgrade().context("Open API session ended")?;
        if !inner.active.load(Ordering::Acquire) {
            bail!("Open API session unavailable");
        }
        match self.receiver.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => bail!("Open API events closed"),
        }
    }
}

impl Drop for OpenApiEvents {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade()
            && let Ok(mut subscribers) = inner.subscribers.lock()
        {
            subscribers.retain(|(id, _)| *id != self.subscription_id);
        }
    }
}

#[derive(Debug)]
struct ReceivedResponse {
    payload_type: u32,
    payload: Value,
    receive_mono_ns: u64,
}
struct Pending {
    expected_type: u32,
    response: oneshot::Sender<Result<ReceivedResponse>>,
    deadline: Instant,
}

struct Inner {
    writer: Mutex<Writer>,
    runtime: tokio::runtime::Handle,
    pending: Mutex<HashMap<String, Pending>>,
    events: std::sync::Mutex<Option<mpsc::Receiver<OpenApiEvent>>>,
    subscribers: std::sync::Mutex<Vec<(u64, mpsc::Sender<OpenApiEvent>)>>,
    next_subscriber: AtomicU64,
    event_capacity: usize,
    event_wire_limit: usize,
    active: Arc<AtomicBool>,
    failure: Mutex<Option<String>>,
    sequence: AtomicU64,
    in_flight: Semaphore,
    request_timeout: Duration,
    max_in_flight: usize,
    account_id: AtomicU64,
    account_revision: AtomicU64,
    trading_scope: AtomicBool,
    expected_trader_login: u64,
    client_handles: AtomicUsize,
    tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Inner {
    fn abort_tasks(&self) {
        if let Ok(mut tasks) = self.tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }

    async fn check_healthy(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            bail!("Open API session unavailable");
        }
        if let Some(reason) = self.failure.lock().await.as_deref() {
            bail!("Open API session unavailable: {reason}");
        }
        Ok(())
    }

    async fn fail(&self, reason: &'static str) {
        let mut failure = self.failure.lock().await;
        if failure.is_some() {
            return;
        }
        *failure = Some(reason.to_owned());
        self.active.store(false, Ordering::Release);
        if let Ok(mut subscribers) = self.subscribers.lock() {
            subscribers.clear();
        }
        drop(failure);
        let mut pending = self.pending.lock().await;
        for (_, request) in pending.drain() {
            let _ = request
                .response
                .send(Err(anyhow!("Open API session unavailable: {reason}")));
        }
        drop(pending);
        let _ = timeout(Duration::from_secs(1), self.writer.lock().await.close()).await;
        self.abort_tasks();
    }
}

/// Shared account-data client. Clones use the same authenticated socket and rate gates.
pub struct OpenApiClient {
    inner: Arc<Inner>,
}

impl Clone for OpenApiClient {
    fn clone(&self) -> Self {
        self.inner.client_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for OpenApiClient {
    fn drop(&mut self) {
        if self.inner.client_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.request_close();
        }
    }
}

impl OpenApiClient {
    /// Establishes TLS, authenticates the app and one exact broker login, then returns a usable client.
    pub async fn connect(config: OpenApiConfig, credentials: OpenApiCredentials) -> Result<Self> {
        let endpoint = config.environment.endpoint();
        Self::connect_to(endpoint, config, credentials).await
    }

    pub(crate) async fn connect_to(
        endpoint: &str,
        config: OpenApiConfig,
        credentials: OpenApiCredentials,
    ) -> Result<Self> {
        config.validate()?;
        let mut websocket_config = WebSocketConfig::default();
        websocket_config.max_message_size = Some(MAX_MESSAGE_BYTES);
        websocket_config.max_frame_size = Some(MAX_MESSAGE_BYTES);
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let (socket, _) = timeout(
            config.request_timeout,
            connect_async_tls_with_config(
                endpoint,
                Some(websocket_config),
                false,
                Some(Connector::Rustls(Arc::new(tls_config))),
            ),
        )
        .await
        .context("Open API connection timed out")?
        .context("Open API connection failed")?;
        let (writer, mut reader) = socket.split();
        let (events_tx, events_rx) = mpsc::channel(config.event_capacity);
        let inner = Arc::new(Inner {
            writer: Mutex::new(writer),
            runtime: tokio::runtime::Handle::current(),
            pending: Mutex::new(HashMap::new()),
            events: std::sync::Mutex::new(Some(events_rx)),
            subscribers: std::sync::Mutex::new(vec![(0, events_tx)]),
            next_subscriber: AtomicU64::new(1),
            event_capacity: config.event_capacity,
            event_wire_limit: config.max_unsolicited_event_bytes,
            active: Arc::new(AtomicBool::new(true)),
            failure: Mutex::new(None),
            sequence: AtomicU64::new(1),
            in_flight: Semaphore::new(config.max_in_flight),
            request_timeout: config.request_timeout,
            max_in_flight: config.max_in_flight,
            account_id: AtomicU64::new(0),
            account_revision: AtomicU64::new(0),
            trading_scope: AtomicBool::new(false),
            expected_trader_login: config.expected_trader_login,
            client_handles: AtomicUsize::new(1),
            tasks: std::sync::Mutex::new(Vec::with_capacity(2)),
        });
        let receiver_inner = Arc::clone(&inner);
        let reader_task = tokio::spawn(async move {
            while let Some(frame) = reader.next().await {
                let received_at = Instant::now();
                let received_wall_time = SystemTime::now();
                let received_mono_ns = match posix_monotonic_ns() {
                    Ok(value) => value,
                    Err(_) => break,
                };
                let raw = match frame {
                    Ok(Message::Text(text)) => text.to_string(),
                    Ok(Message::Binary(bytes)) => match String::from_utf8(bytes.to_vec()) {
                        Ok(text) => text,
                        Err(_) => break,
                    },
                    Ok(Message::Ping(_) | Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => continue,
                };
                if raw.len() > MAX_MESSAGE_BYTES {
                    break;
                }
                let envelope: Value = match serde_json::from_str(&raw) {
                    Ok(value) => value,
                    Err(_) => break,
                };
                let Some(payload_type) = envelope
                    .get("payloadType")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                else {
                    break;
                };
                if payload_type == HEARTBEAT_TYPE {
                    continue;
                }
                if matches!(payload_type, 50 | 2142)
                    && envelope
                        .get("clientMsgId")
                        .and_then(Value::as_str)
                        .is_none()
                {
                    receiver_inner.fail("uncorrelated Open API error").await;
                    return;
                }
                let payload = match envelope.get("payload") {
                    Some(value) if value.is_object() => value.clone(),
                    None if payload_type == 2101 => json!({}),
                    _ => break,
                };
                // Typed mutation replies remain account events for every subscriber.
                // Its execution/error response remains an account event for every subscriber.
                let mutation_reply = if matches!(payload_type, 2126 | 2132 | 2142 | 50) {
                    if let Some(identity) = envelope.get("clientMsgId").and_then(Value::as_str) {
                        let mut requests = receiver_inner.pending.lock().await;
                        if requests
                            .get(identity)
                            .is_some_and(|pending| pending.expected_type == 2126)
                        {
                            requests.remove(identity)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if mutation_reply.is_none()
                    && let Some(identity) = envelope.get("clientMsgId").and_then(Value::as_str)
                    && !matches!(
                        payload_type,
                        2120 | 2123 | 2126 | 2131 | 2132 | 2141 | 2147 | 2148 | 2155 | 2164
                    )
                {
                    let pending = receiver_inner.pending.lock().await.remove(identity);
                    if let Some(pending) = pending {
                        let result = if payload_type == pending.expected_type {
                            Ok(ReceivedResponse {
                                payload_type,
                                payload,
                                receive_mono_ns: received_mono_ns,
                            })
                        } else if matches!(payload_type, 50 | 2142) {
                            Err(readonly_rejection(&payload))
                        } else {
                            Err(anyhow!("Open API response type mismatch"))
                        };
                        let _ = pending.response.send(result);
                    } else {
                        receiver_inner
                            .fail("unmatched Open API response identity")
                            .await;
                        return;
                    }
                    continue;
                }
                if raw.len() > receiver_inner.event_wire_limit {
                    receiver_inner
                        .fail("Open API event wire budget exceeded")
                        .await;
                    return;
                }
                if matches!(payload_type, 2147 | 2148 | 2164) {
                    receiver_inner
                        .fail("account authorization or connection invalidated")
                        .await;
                    return;
                }
                let account_id = receiver_inner.account_id.load(Ordering::Acquire);
                if account_id != 0
                    && payload
                        .get("ctidTraderAccountId")
                        .is_some_and(|value| wire_u64(value) != Some(account_id))
                {
                    receiver_inner.fail("event account identity mismatch").await;
                    return;
                }
                if mutation_reply.is_some()
                    && matches!(payload_type, 2126 | 2132)
                    && payload.get("ctidTraderAccountId").and_then(wire_u64) != Some(account_id)
                {
                    receiver_inner
                        .fail("mutation response lacks authenticated account identity")
                        .await;
                    return;
                }
                // Market spot and depth events do not mutate account state. Other unsolicited events,
                // including metadata and future account event kinds, invalidate observations.
                // This is reader-owned evidence, shared by all consumers of this epoch.
                let account_revision = if matches!(payload_type, 2131 | 2155) {
                    receiver_inner.account_revision.load(Ordering::Acquire)
                } else {
                    match receiver_inner.account_revision.try_update(
                        Ordering::AcqRel,
                        Ordering::Acquire,
                        |revision| revision.checked_add(1),
                    ) {
                        Ok(previous) => previous + 1,
                        Err(_) => {
                            receiver_inner.fail("account revision exhausted").await;
                            return;
                        }
                    }
                };
                let event = OpenApiEvent {
                    client_message_id: envelope.get("clientMsgId").and_then(Value::as_str).map(str::to_owned),
                    payload_type,
                    payload: payload.clone(),
                    account_revision,
                    received_at,
                    received_wall_time,
                    received_mono_ns,
                };
                let overflow = match receiver_inner.subscribers.lock() {
                    Ok(mut subscribers) => {
                        subscribers.retain(|(_, sender)| !sender.is_closed());
                        subscribers.is_empty()
                            || subscribers.iter().any(|(_, sender)| {
                                matches!(
                                    sender.try_send(event.clone()),
                                    Err(mpsc::error::TrySendError::Full(_))
                                )
                            })
                    }
                    Err(_) => true,
                };
                if overflow {
                    receiver_inner
                        .fail("event subscriber overflow or closed during delivery")
                        .await;
                    return;
                }
                if let Some(pending) = mutation_reply {
                    let _ = pending.response.send(Ok(ReceivedResponse {
                        payload_type,
                        payload,
                        receive_mono_ns: received_mono_ns,
                    }));
                }
            }
            receiver_inner.fail("socket reader stopped").await;
        });
        let heartbeat_inner = Arc::clone(&inner);
        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(10));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if heartbeat_inner.check_healthy().await.is_err() {
                    break;
                }
                let now = Instant::now();
                let mut pending = heartbeat_inner.pending.lock().await;
                let expired: Vec<_> = pending
                    .iter()
                    .filter(|(_, request)| request.deadline <= now)
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in expired {
                    if let Some(request) = pending.remove(&id) {
                        let _ = request
                            .response
                            .send(Err(anyhow!("Open API response timed out")));
                    }
                }
                drop(pending);
                let frame = Message::Text(
                    json!({"payloadType": HEARTBEAT_TYPE, "payload": {}})
                        .to_string()
                        .into(),
                );
                let sent = timeout(
                    heartbeat_inner.request_timeout,
                    heartbeat_inner.writer.lock().await.send(frame),
                )
                .await;
                if !matches!(sent, Ok(Ok(()))) {
                    heartbeat_inner.fail("heartbeat send failed").await;
                    break;
                }
            }
        });
        inner
            .tasks
            .lock()
            .map_err(|_| anyhow!("Open API task lock poisoned"))?
            .extend([reader_task, heartbeat_task]);
        let client = Self { inner };
        if let Err(e) = client.authenticate(&config, &credentials).await {
            client.inner.fail("authentication failed").await;
            return Err(e);
        }
        Ok(client)
    }

    async fn authenticate(
        &self,
        config: &OpenApiConfig,
        credentials: &OpenApiCredentials,
    ) -> Result<()> {
        self.raw_request(
            2100,
            2101,
            json!({"clientId": credentials.client_id,
            "clientSecret": credentials.client_secret}),
        )
        .await?;
        let accounts = self
            .raw_request(
                2149,
                2150,
                json!({"accessToken": credentials.access_token}),
            )
            .await?;
        if accounts.get("accessToken").and_then(Value::as_str)
            != Some(credentials.access_token.as_str())
        {
            bail!("Open API token response identity mismatch");
        }
        // Read-only consumers work under VIEW or TRADE. Only the explicitly typed
        // startup ClosePosition endpoint can use TRADE; ReadOnlyRequest stays read-only.
        let permission = match accounts.get("permissionScope") {
            Some(Value::String(scope)) if scope == "SCOPE_VIEW" => Some(0),
            Some(Value::String(scope)) if scope == "SCOPE_TRADE" => Some(1),
            Some(value) => wire_u64(value),
            None => None,
        };
        match permission {
            Some(0) => self.inner.trading_scope.store(false, Ordering::Release),
            Some(1) => self.inner.trading_scope.store(true, Ordering::Release),
            _ => bail!("Open API token permission scope is absent or unsupported"),
        }
        let granted = accounts
            .get("ctidTraderAccount")
            .and_then(Value::as_array)
            .context("Open API account list missing")?;
        let matching: Vec<u64> = granted
            .iter()
            .filter_map(|account| {
                let login = wire_u64(account.get("traderLogin")?)?;
                let is_live = account.get("isLive")?.as_bool()?;
                (login == config.expected_trader_login && is_live == config.environment.is_live())
                    .then(|| wire_u64(account.get("ctidTraderAccountId")?))
                    .flatten()
            })
            .collect();
        if matching.len() != 1 || matching[0] == 0 {
            bail!("Open API granted account identity is absent or ambiguous");
        }
        let account_id = matching[0];
        let authorized = self
            .raw_request(
                2102,
                2103,
                json!({"ctidTraderAccountId": account_id,
                "accessToken": credentials.access_token}),
            )
            .await?;
        if authorized.get("ctidTraderAccountId").and_then(wire_u64) != Some(account_id) {
            bail!("Open API account authorization identity mismatch");
        }
        let response = self
            .raw_request(
                2121,
                2122,
                json!({"ctidTraderAccountId": account_id}),
            )
            .await?;
        let trader = response
            .get("trader")
            .context("Open API trader details missing")?;
        if response.get("ctidTraderAccountId").and_then(wire_u64) != Some(account_id)
            || trader.get("ctidTraderAccountId").and_then(wire_u64) != Some(account_id)
            || trader.get("traderLogin").and_then(wire_u64) != Some(config.expected_trader_login)
        {
            bail!("Open API trader identity mismatch");
        }
        self.inner.account_id.store(account_id, Ordering::Release);
        Ok(())
    }

    async fn raw_request(
        &self,
        request_type: u32,
        response_type: u32,
        payload: Value,
    ) -> Result<Value> {
        self.inner.check_healthy().await?;
        let _permit = timeout(self.inner.request_timeout, self.inner.in_flight.acquire())
            .await
            .context("Open API admission timed out")??;
        self.inner.check_healthy().await?;
        let id = self
            .inner
            .sequence
            .fetch_add(1, Ordering::Relaxed)
            .to_string();
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.inner.pending.lock().await;
        if pending.len() >= self.inner.max_in_flight {
            bail!("Open API pending request capacity exhausted");
        }
        pending.insert(
            id.clone(),
            Pending {
                expected_type: response_type,
                response: sender,
                deadline: Instant::now() + self.inner.request_timeout,
            },
        );
        drop(pending);
        let message = Message::Text(
            json!({"clientMsgId": id,
            "payloadType": request_type, "payload": payload})
            .to_string()
            .into(),
        );
        let sent = timeout(
            self.inner.request_timeout,
            self.inner.writer.lock().await.send(message),
        )
        .await;
        if !matches!(sent, Ok(Ok(()))) {
            self.inner.pending.lock().await.remove(&id);
            self.inner.fail("socket writer stopped").await;
            bail!("Open API socket writer stopped");
        }
        let answer = timeout(self.inner.request_timeout, receiver).await;
        self.inner.pending.lock().await.remove(&id);
        let payload = answer
            .context("Open API response timed out")?
            .context("Open API response channel closed")??;
        // A reply may already have been queued when another frame invalidates
        // this epoch. Never return that queued reply as a healthy observation.
        self.inner.check_healthy().await?;
        Ok(payload.payload)
    }

    /// Reader-observed account event revision within this socket epoch.
    ///
    /// Every accepted unsolicited event advances the revision before fanout. A
    /// stable value is only one part of a valid snapshot: callers must also check
    /// the connection epoch, health, complete broker snapshots, and observation age.
    pub fn account_revision(&self) -> u64 {
        self.inner.account_revision.load(Ordering::Acquire)
    }
    pub(crate) fn authenticated_account_id(&self) -> Result<u64> {
        let account = self.inner.account_id.load(Ordering::Acquire);
        if account == 0 || !self.inner.active.load(Ordering::Acquire) {
            bail!("Open API account is not authenticated");
        }
        Ok(account)
    }
    pub(super) fn validate_event_wire_budget(
        &self,
        max_event_bytes: usize,
        max_queued_wire_bytes: usize,
    ) -> Result<()> {
        anyhow::ensure!(
            (1024..=MAX_MESSAGE_BYTES).contains(&max_event_bytes)
                && self
                    .inner
                    .event_capacity
                    .checked_mul(max_event_bytes)
                    .is_some_and(|bytes| bytes <= max_queued_wire_bytes),
            "Open API event queue wire budget exceeded"
        );
        anyhow::ensure!(
            self.inner.event_wire_limit <= max_event_bytes,
            "Open API event limit was not set before authentication"
        );
        Ok(())
    }

    pub(crate) fn activity_fence(&self) -> Arc<AtomicBool> {
        self.inner.active.clone()
    }

    /// Returns whether account authentication is complete and the session has not failed.
    pub async fn is_connected(&self) -> bool {
        self.inner.account_id.load(Ordering::Acquire) != 0
            && self.inner.check_healthy().await.is_ok()
    }

    /// Sends an account-bound request from the read-only Open API command set.
    pub async fn request(&self, operation: ReadOnlyRequest, mut payload: Value) -> Result<Value> {
        let account_id = self.inner.account_id.load(Ordering::Acquire);
        if account_id == 0 {
            bail!("Open API account is unauthenticated");
        }
        let object = payload
            .as_object_mut()
            .context("Open API request payload must be an object")?;
        if let Some(other) = object.get("ctidTraderAccountId")
            && wire_u64(other) != Some(account_id)
        {
            bail!("Open API account identity mismatch");
        }
        object.insert("ctidTraderAccountId".to_owned(), json!(account_id));
        let response = self
            .raw_request(
                operation.request_type(),
                operation.response_type(),
                payload,
            )
            .await?;
        if response.get("ctidTraderAccountId").and_then(wire_u64) != Some(account_id) {
            bail!("Open API response account identity mismatch");
        }
        Ok(response)
    }

    /// Retrieves only the broker balance observation; equity remains unknown until separately reconciled.
    pub async fn account_snapshot(&self) -> Result<BrokerAccountSnapshot> {
        let response = self.request(ReadOnlyRequest::Trader, json!({})).await?;
        let trader = response
            .get("trader")
            .context("Open API trader details missing")?;
        let account = BrokerAccountSnapshot::from_trader(trader)?;
        if account.ctid_trader_account_id != self.inner.account_id.load(Ordering::Acquire)
            || account.trader_login != self.inner.expected_trader_login
        {
            bail!("Open API trader identity changed");
        }
        Ok(account)
    }

    /// Retrieves all deals in an inclusive millisecond interval, splitting truncated windows.
    pub async fn deals_complete(&self, from_ms: i64, to_ms: i64) -> Result<Vec<BrokerDeal>> {
        self.deals_complete_bounded(from_ms, to_ms, MAX_HISTORY_DEALS)
            .await
    }

    /// Retrieves complete history within the caller's existing record budget.
    ///
    /// # Errors
    /// Returns the existing resource error before retaining an additional unique deal.
    pub(crate) async fn deals_complete_bounded(
        &self,
        from_ms: i64,
        to_ms: i64,
        bound: usize,
    ) -> Result<Vec<BrokerDeal>> {
        if !(1..=MAX_HISTORY_DEALS).contains(&bound) {
            bail!("invalid Open API deal history memory bound");
        }
        if from_ms < 0 || to_ms < from_ms || to_ms > 2_147_483_646_000 {
            bail!("invalid Open API deal history interval");
        }
        let mut windows = vec![(from_ms, to_ms)];
        let mut deals = HashMap::<u64, BrokerDeal>::new();
        let mut visited = 0_u32;
        while let Some((from, to)) = windows.pop() {
            visited += 1;
            if visited > 16_384 {
                bail!("Open API deal history exceeds bounded pagination");
            }
            let response = self
                .request(
                    ReadOnlyRequest::Deals,
                    json!({"fromTimestamp": from, "toTimestamp": to, "maxRows": 1000}),
                )
                .await?;
            let rows = match response.get("deal") {
                None => &[][..],
                Some(Value::Array(rows)) => rows.as_slice(),
                _ => bail!("Malformed Open API deal history rows"),
            };
            extend_history_deals(&mut deals, rows, bound)?;
            let more = response
                .get("hasMore")
                .and_then(Value::as_bool)
                .context("Open API history pagination indicator missing")?;
            if more {
                if from == to {
                    bail!("Open API history cannot be paginated within one millisecond");
                }
                let middle = from + (to - from) / 2;
                windows.push((middle + 1, to));
                windows.push((from, middle));
            }
        }
        let mut sorted: Vec<_> = deals.into_values().collect();
        sorted.sort_by_key(|deal| (deal.execution_timestamp_ms, deal.deal_id));
        Ok(sorted)
    }

    /// Takes the bootstrap queue, including events received during authentication.
    ///
    /// # Errors
    /// Returns an error if the bootstrap receiver was already taken.
    pub fn take_events(&self) -> Result<OpenApiEvents> {
        let receiver = self
            .inner
            .events
            .lock()
            .map_err(|_| anyhow!("Open API event lock poisoned"))?
            .take()
            .context("Open API bootstrap events already taken")?;
        Ok(OpenApiEvents {
            receiver,
            inner: Arc::downgrade(&self.inner),
            subscription_id: 0,
        })
    }
    /// Starts an independent bounded queue before this consumer's snapshot requests.
    ///
    /// # Errors
    /// Returns an error if the epoch failed or subscriber capacity is exhausted.
    pub(super) fn subscribe_events(&self) -> Result<OpenApiEvents> {
        let mut subscribers = self
            .inner
            .subscribers
            .lock()
            .map_err(|_| anyhow!("Open API event lock poisoned"))?;
        if !self.inner.active.load(Ordering::Acquire) {
            bail!("Open API session unavailable");
        }
        let (sender, receiver) = mpsc::channel(self.inner.event_capacity);
        subscribers.retain(|(_, sender)| !sender.is_closed());
        anyhow::ensure!(
            subscribers.len() < MAX_EVENT_SUBSCRIBERS,
            "Open API subscriber capacity exhausted"
        );
        let id = self
            .inner
            .next_subscriber
            .try_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
            .map_err(|_| anyhow!("Open API subscriber identity exhausted"))?;
        subscribers.push((id, sender));
        Ok(OpenApiEvents {
            receiver,
            inner: Arc::downgrade(&self.inner),
            subscription_id: id,
        })
    }
    pub(super) fn unsubscribe_events(&self, id: u64) {
        if let Ok(mut subscribers) = self.inner.subscribers.lock() {
            subscribers.retain(|(registered, _)| *registered != id);
        }
    }
    /// Ends this socket epoch synchronously; escaped client clones fail immediately.
    pub fn request_close(&self) {
        if !self.inner.active.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut subscribers) = self.inner.subscribers.lock() {
            subscribers.clear();
        }
        self.inner.abort_tasks();
        let inner = Arc::clone(&self.inner);
        self.inner.runtime.spawn(async move {
            inner.pending.lock().await.clear();
            let _ = timeout(Duration::from_secs(1), inner.writer.lock().await.close()).await;
        });
    }

    /// Closes the shared session; every clone thereafter fails closed.
    pub async fn close(&self) {
        self.inner.fail("client closed").await;
    }
}

fn wire_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn readonly_rejected_request_preserves_only_bounded_protocol_code() {
        let safe = super::readonly_rejection(&serde_json::json!({
            "errorCode":"NOT_AUTHORIZED", "description":"fixture-private-description",
            "ctidTraderAccountId":42}));
        assert_eq!(safe.to_string(), "Open API rejected request [errorCode=NOT_AUTHORIZED]");
        for code in ["", "unsafe description", "token\nvalue", &"X".repeat(129)] {
            assert_eq!(super::readonly_rejection(&serde_json::json!({"errorCode":code}))
                .to_string(), "Open API rejected request");
        }
        assert_eq!(super::readonly_rejection(&serde_json::json!({})).to_string(),
            "Open API rejected request");
    }
    use super::*;
    use rust_decimal::Decimal;
    use tokio::{net::TcpListener, time::timeout};
    use tokio_tungstenite::accept_async;

    fn history_row(deal_id: u64) -> Value {
        json!({"dealId":deal_id,"orderId":1,"positionId":1,"symbolId":1,
            "volume":100,"filledVolume":100,"tradeSide":1,"dealStatus":2,
            "createTimestamp":1000,"executionTimestamp":1001,"executionPrice":"1.25"})
    }

    #[test]
    fn history_prebound_stops_large_page_before_decoding_overflow_row() {
        let mut deals = HashMap::new();
        let mut rows: Vec<_> = (1..=32).map(history_row).collect();
        // The next row cannot be decoded, so a bound error proves the pre-decode check
        rows.push(json!({"dealId":33}));
        rows.extend((34..4096).map(history_row));
        let e = extend_history_deals(&mut deals, &rows, 32).unwrap_err();
        assert_eq!(e.to_string(), "Open API deal history exceeds memory bound");
        assert_eq!(deals.len(), 32);
    }

    #[test]
    fn history_prebound_preserves_last_equal_decimal_representation() {
        let mut deals = HashMap::new();
        let first = history_row(1);
        let mut last = first.clone();
        last["executionPrice"] = json!("1.250");
        extend_history_deals(&mut deals, &[first, last], 1).unwrap();
        assert_eq!(deals[&1].execution_price.unwrap().to_string(), "1.250");
    }

    #[test]
    fn history_prebound_accumulates_pages_and_accepts_exact_replay_at_capacity() {
        let mut deals = HashMap::new();
        extend_history_deals(&mut deals, &[history_row(1), history_row(2)], 3).unwrap();
        extend_history_deals(&mut deals, &[history_row(1), history_row(3)], 3).unwrap();
        assert_eq!(deals.len(), 3);
        let e = extend_history_deals(&mut deals, &[json!({"dealId":4})], 3).unwrap_err();
        assert_eq!(e.to_string(), "Open API deal history exceeds memory bound");
        assert_eq!(deals.len(), 3);
        let mut conflict = history_row(1);
        conflict["executionPrice"] = json!("2.5");
        assert_eq!(
            extend_history_deals(&mut deals, &[conflict], 3)
                .unwrap_err()
                .to_string(),
            "Open API duplicate deal disagrees",
        );
    }

    async fn mock_endpoint(scope: Option<u32>, login: u64) -> (String, oneshot::Receiver<()>) {
        mock_endpoint_with_events(scope, login, vec![2123], false).await
    }

    async fn mock_endpoint_with_events(
        scope: Option<u32>,
        login: u64,
        event_types: Vec<u32>,
        correlated_event: bool,
    ) -> (String, oneshot::Receiver<()>) {
        mock_endpoint_with_payload_omission(scope, login, event_types, correlated_event, None).await
    }

    async fn mock_endpoint_with_payload_omission(
        scope: Option<u32>,
        login: u64,
        event_types: Vec<u32>,
        correlated_event: bool,
        omit_payload_for: Option<u64>,
    ) -> (String, oneshot::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (closed_tx, closed_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let request: Value = serde_json::from_str(&text).unwrap();
                let kind = request["payloadType"].as_u64().unwrap();
                if kind == u64::from(HEARTBEAT_TYPE) {
                    continue;
                }
                if matches!(kind, 2156 | 2158) {
                    assert_eq!(
                        wire_u64(&request["payload"]["ctidTraderAccountId"]),
                        Some(42)
                    );
                    assert_eq!(request["payload"]["symbolId"], json!([7]));
                }
                let (response_type, payload) = match kind {
                    2100 => (2101, json!({})),
                    2149 => (
                        2150,
                        json!({"accessToken": "mock-token", "permissionScope": scope,
                        "ctidTraderAccount": [{"ctidTraderAccountId": "42",
                            "traderLogin": login.to_string(), "isLive": false}]}),
                    ),
                    2102 => (2103, json!({"ctidTraderAccountId": "42"})),
                    2133 => (2134, json!({"ctidTraderAccountId": "42", "hasMore": false})),
                    2156 => (2157, json!({"ctidTraderAccountId": "42"})),
                    2158 => (2159, json!({"ctidTraderAccountId": "42"})),
                    2121 => (
                        2122,
                        json!({"ctidTraderAccountId": "42", "trader": {
                        "ctidTraderAccountId": "42", "traderLogin": login.to_string(),
                        "depositAssetId": "1", "balance": "10053099944", "moneyDigits": 8}}),
                    ),
                    other => panic!("unexpected mock request type {other}"),
                };
                if request["payload"]["hold"] == true {
                    let _ = socket
                        .send(Message::Text(
                            json!({
                                "payloadType":2123,
                                "payload":{"ctidTraderAccountId":"42","marker":"held-request"},
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                    continue;
                }
                let mut response = json!({"clientMsgId": request["clientMsgId"],
                    "payloadType": response_type, "payload": payload});
                if let Some(error_type) = request["payload"]["mockRejectType"].as_u64() {
                    response = json!({"clientMsgId":request["clientMsgId"],
                        "payloadType":error_type,"payload":{
                        "errorCode":"CH_DEPTH_QUOTES_NOT_SUPPORTED",
                        "description":"fixture-private-description"}});
                }
                if omit_payload_for == Some(kind) {
                    response.as_object_mut().unwrap().remove("payload");
                }
                if request["payload"]["largeResponse"] == true {
                    response["payload"]["padding"] = json!("x".repeat(1_500_000));
                }
                socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .unwrap();
                if kind == 2156 && request["payload"]["mockRejectType"].is_null() {
                    socket
                        .send(Message::Text(
                            json!({"payloadType":2155,"payload":{
                                "ctidTraderAccountId":"42","symbolId":"7",
                                "newQuotes":[{"id":"1","size":"100","bid":"123456"},
                                             {"id":"2","size":"50","ask":"123500"}]
                            }})
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
                if kind == 2121 {
                    for event_type in &event_types {
                        let mut event = json!({"payloadType": event_type,
                            "payload": {"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42"}}});
                        if correlated_event {
                            event["clientMsgId"] = request["clientMsgId"].clone();
                        }
                        if socket
                            .send(Message::Text(event.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
            let _ = closed_tx.send(());
        });
        (endpoint, closed_rx)
    }

    fn dummy_credentials() -> OpenApiCredentials {
        OpenApiCredentials {
            client_id: "mock-id".to_owned(),
            client_secret: "mock-secret".to_owned(),
            access_token: "mock-token".to_owned(),
        }
    }

    #[tokio::test]
    async fn readonly_depth_correlated_errors_keep_only_protocol_code_and_healthy_epoch() {
        for error_type in [50, 2142] {
            let (endpoint, closed) = mock_endpoint_with_events(Some(0), 100_001, vec![], false).await;
            let client = OpenApiClient::connect_to(&endpoint,
                OpenApiConfig::demo(100_001), dummy_credentials()).await.unwrap();
            let error = client.request(ReadOnlyRequest::SubscribeDepthQuotes,
                json!({"symbolId":[7],"mockRejectType":error_type})).await.unwrap_err();
            assert_eq!(error.to_string(),
                "Open API rejected request [errorCode=CH_DEPTH_QUOTES_NOT_SUPPORTED]");
            assert!(client.account_snapshot().await.is_ok());
            client.close().await;
            timeout(Duration::from_secs(2),closed).await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn depth_subscription_uses_authenticated_account_and_read_only_transport() {
        let (endpoint, closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![], false).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        for kind in [
            ReadOnlyRequest::SubscribeDepthQuotes,
            ReadOnlyRequest::UnsubscribeDepthQuotes,
        ] {
            let reply = client
                .request(kind, json!({"symbolId": [7]}))
                .await
                .unwrap();
            assert_eq!(reply["ctidTraderAccountId"], "42");
        }
        assert_eq!(client.account_revision(), 0);
        client.close().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn depth_event_flows_from_authenticated_socket_to_bounded_book() {
        use crate::openapi::{depth::DepthBook, depth_wire::apply_event};
        let (endpoint, closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![], false).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let mut events = client.take_events().unwrap();
        client
            .request(
                ReadOnlyRequest::SubscribeDepthQuotes,
                json!({"symbolId":[7]}),
            )
            .await
            .unwrap();
        let event = timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.payload_type, 2155);
        let mut book = DepthBook::new(7, 10).unwrap();
        apply_event(&mut book, 42, 7, &event.payload).unwrap();
        assert_eq!(
            book.levels().unwrap(),
            (&[(123_456, 100)][..], &[(123_500, 50)][..])
        );
        assert_eq!(client.account_revision(), 0);
        client.close().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stalled_separate_market_epoch_does_not_close_execution_epoch() {
        let (market_endpoint, market_closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![2155], false).await;
        let (account_endpoint, account_closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![], false).await;
        let mut config = OpenApiConfig::demo(100_001);
        config.event_capacity = 1;
        let market = OpenApiClient::connect_to(&market_endpoint, config, dummy_credentials())
            .await
            .unwrap();
        let account = OpenApiClient::connect_to(
            &account_endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let _ = market.account_snapshot().await;
        timeout(Duration::from_secs(2), market_closed)
            .await
            .unwrap()
            .unwrap();
        assert!(!market.is_connected().await);
        assert!(account.is_connected().await);
        account.account_snapshot().await.unwrap();
        account.close().await;
        timeout(Duration::from_secs(2), account_closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn accepts_empty_application_auth_response_without_payload() {
        let (endpoint, _) =
            mock_endpoint_with_payload_omission(Some(0), 100_001, vec![], false, Some(2100))
                .await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        assert!(client.is_connected().await);
        client.close().await;
    }

    #[tokio::test]
    async fn rejects_missing_nonempty_trader_response_payload() {
        let (endpoint, _) =
            mock_endpoint_with_payload_omission(Some(0), 100_001, vec![], false, Some(2121))
                .await;
        assert!(
            OpenApiClient::connect_to(
                &endpoint,
                OpenApiConfig::demo(100_001),
                dummy_credentials(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn tls_cold_start_returns_connection_error_without_provider_panic() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("wss://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });
        assert!(
            OpenApiClient::connect_to(
                &endpoint,
                OpenApiConfig::demo(100_001),
                dummy_credentials(),
            )
            .await
            .is_err()
        );
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn valid_history_page_larger_than_one_megabyte_keeps_session_readable() {
        let (endpoint, _) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let response = client
            .request(
                ReadOnlyRequest::Deals,
                json!({"fromTimestamp":1000,"toTimestamp":2000,"largeResponse":true}),
            )
            .await
            .unwrap();
        assert_eq!(response["padding"].as_str().unwrap().len(), 1_500_000);
        assert!(client.account_snapshot().await.is_ok());
        client.close().await;
    }

    #[tokio::test]
    async fn empty_deal_history_accepts_omitted_repeated_rows() {
        let (endpoint, _) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        assert!(client.deals_complete(1000, 2000).await.unwrap().is_empty());
        client.close().await;
    }

    #[tokio::test]
    async fn authenticates_exact_demo_login_and_receives_background_event() {
        let (endpoint, mut closed) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        assert!(client.is_connected().await);
        let mut events = client.take_events().unwrap();
        let snapshot = client.account_snapshot().await.unwrap();
        assert_eq!(snapshot.balance, Decimal::new(10_053_099_944, 8));
        let event = timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.payload_type, 2123);
        assert!(event.received_wall_time <= SystemTime::now());
        assert!(event.received_at <= Instant::now());
        assert!(event.received_mono_ns > 0);
        assert!(
            event
                .received_mono_ns
                .abs_diff(crate::clock::monotonic_ns().unwrap())
                < 1_000_000_000
        );
        let clone = client.clone();
        drop(client);
        assert!(
            timeout(Duration::from_millis(100), &mut closed)
                .await
                .is_err()
        );
        clone.close().await;
        assert!(!clone.is_connected().await);
        timeout(Duration::from_secs(1), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_unknown_scope_and_wrong_broker_login() {
        let (endpoint, closed) = mock_endpoint(Some(2), 100_001).await;
        assert!(
            OpenApiClient::connect_to(
                &endpoint,
                OpenApiConfig::demo(100_001),
                dummy_credentials()
            )
            .await
            .is_err()
        );
        timeout(Duration::from_secs(1), closed)
            .await
            .unwrap()
            .unwrap();
        let (endpoint, closed) = mock_endpoint(Some(0), 123).await;
        assert!(
            OpenApiClient::connect_to(
                &endpoint,
                OpenApiConfig::demo(100_001),
                dummy_credentials()
            )
            .await
            .is_err()
        );
        timeout(Duration::from_secs(1), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn final_handle_drop_and_event_overflow_close_the_peer() {
        let (endpoint, closed) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        drop(client);
        timeout(Duration::from_secs(1), closed)
            .await
            .unwrap()
            .unwrap();

        let (endpoint, closed) = mock_endpoint(Some(0), 100_001).await;
        let mut config = OpenApiConfig::demo(100_001);
        config.event_capacity = 1;
        let client = OpenApiClient::connect_to(&endpoint, config, dummy_credentials())
            .await
            .unwrap();
        let _ = client.account_snapshot().await;
        timeout(Duration::from_secs(1), closed)
            .await
            .unwrap()
            .unwrap();
        assert!(client.account_snapshot().await.is_err());
    }
    #[rstest::rstest]
    #[case(2120, false)]
    #[case(2123, false)]
    #[case(2126, false)]
    #[case(2141, false)]
    #[case(2999, false)]
    #[case(2123, true)]
    #[case(2126, true)]
    #[tokio::test]
    async fn reader_advances_account_revision_before_delivering_events(
        #[case] event_type: u32,
        #[case] correlated_event: bool,
    ) {
        let (endpoint, closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![event_type], correlated_event)
                .await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let clone = client.clone();
        let mut events = client.take_events().unwrap();
        let first = timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.payload_type, event_type);
        assert_eq!(first.account_revision, 1);
        assert_eq!(client.account_revision(), 1);
        client.account_snapshot().await.unwrap();
        let second = timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.account_revision, 2);
        assert_eq!(client.account_revision(), 2);
        assert_eq!(clone.account_revision(), 2);
        client.close().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[rstest::rstest]
    #[case(51, false)]
    #[case(2131, false)]
    #[case(2155, false)]
    #[case(2155, true)]
    #[tokio::test]
    async fn responses_and_market_events_do_not_advance_account_revision(
        #[case] event_type: u32,
        #[case] correlated_event: bool,
    ) {
        let (endpoint, closed) =
            mock_endpoint_with_events(Some(0), 100_001, vec![event_type], correlated_event)
                .await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        client.account_snapshot().await.unwrap();
        if event_type != 51 {
            let mut events = client.take_events().unwrap();
            timeout(Duration::from_secs(2), events.next())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(client.account_revision(), 0);
        assert!(client.is_connected().await);
        client.close().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn account_revision_overflow_fails_the_epoch_without_wrapping() {
        let (endpoint, closed) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let mut events = client.take_events().unwrap();
        timeout(Duration::from_secs(2), events.next())
            .await
            .unwrap()
            .unwrap();
        client
            .inner
            .account_revision
            .store(u64::MAX, Ordering::Release);
        let _ = client.account_snapshot().await;
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client.account_revision(), u64::MAX);
        assert!(!client.is_connected().await);
        assert!(events.next().await.is_none());
    }
    #[tokio::test]
    async fn a_queued_success_response_cannot_escape_an_already_closed_epoch() {
        let (endpoint, closed) = mock_endpoint(Some(0), 100_001).await;
        let client = OpenApiClient::connect_to(
            &endpoint,
            OpenApiConfig::demo(100_001),
            dummy_credentials(),
        )
        .await
        .unwrap();
        let mut events = client.take_events().unwrap();
        let querying_client = client.clone();
        let query = tokio::spawn(async move {
            querying_client
                .request(ReadOnlyRequest::Trader, json!({"hold":true}))
                .await
        });
        timeout(Duration::from_secs(2), async {
            loop {
                let event = events.next().await.unwrap();
                if event.payload["marker"] == "held-request" {
                    break;
                }
            }
        })
        .await
        .unwrap();
        // The peer confirmed reception and is deliberately withholding its reply.
        // Resolve the actual request oneshot only after the epoch is synchronously
        // invalidated, reproducing a queued response racing connection failure.
        let response = client
            .inner
            .pending
            .lock()
            .await
            .drain()
            .next()
            .unwrap()
            .1
            .response;
        client.request_close();
        response
            .send(Ok(ReceivedResponse {
                payload_type: 2122,
                payload: json!({"ctidTraderAccountId":"42"}),
                receive_mono_ns: posix_monotonic_ns().unwrap(),
            }))
            .unwrap();
        assert!(
            timeout(Duration::from_secs(2), query)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        timeout(Duration::from_secs(2), closed)
            .await
            .unwrap()
            .unwrap();
    }
}

#[path = "order.rs"]
mod order;
pub(crate) use order::{
    CancelOrderRequest, MarketOrderRequest, OrderMutationReply, PositionCloseRequest,
};

