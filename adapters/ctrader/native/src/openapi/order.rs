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

//! Fixed Open API MARKET/IOC and cancel transport; durable intent belongs to the caller.
use super::super::OpenApiAdmission;
use super::*;
use anyhow::ensure;
use futures_util::{Sink, future::poll_fn};
use std::{fmt, pin::Pin, task::Poll};

/// Market and cancellation commands cannot encode arbitrary protocol writes.
pub(crate) struct MarketOrderRequest {
    pub expected_account_id: u64,
    pub expected_revision: u64,
    pub symbol_id: u64,
    pub side: u32,
    pub volume: i64,
    pub position_id: Option<u64>,
    pub client_order_id: String,
    pub client_message_id: String,
}
pub(crate) struct PositionCloseRequest {
    pub expected_account_id: u64,
    pub expected_revision: u64,
    pub position_id: u64,
    pub volume: i64,
    pub client_message_id: String,
}
pub(crate) struct CancelOrderRequest {
    pub expected_account_id: u64,
    pub expected_revision: u64,
    pub order_id: u64,
    pub client_message_id: String,
}
fn valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 50 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
}
fn valid_number(n: u64) -> bool {
    n > 0 && i64::try_from(n).is_ok()
}
impl MarketOrderRequest {
    fn validate(&self) -> Result<()> {
        ensure!(
            valid_number(self.expected_account_id)
                && valid_number(self.symbol_id)
                && matches!(self.side, 1 | 2)
                && self.volume > 0
                && self.position_id.is_none_or(valid_number)
                && valid_id(&self.client_order_id)
                && valid_id(&self.client_message_id),
            "Invalid market order request"
        );
        Ok(())
    }
}
impl CancelOrderRequest {
    fn validate(&self) -> Result<()> {
        ensure!(
            valid_number(self.expected_account_id)
                && valid_number(self.order_id)
                && valid_id(&self.client_message_id),
            "Invalid cancel order request"
        );
        Ok(())
    }
}
struct OrderFrame {
    expected_account_id: u64,
    expected_revision: u64,
    client_message_id: String,
    payload_type: u32,
    payload: Value,
}
#[derive(Debug)]
pub(crate) struct OrderMutationSendError {
    pub may_have_been_sent: bool,
    pub reason: &'static str,
}
impl fmt::Display for OrderMutationSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (may_have_been_sent={})",
            self.reason, self.may_have_been_sent
        )
    }
}
impl std::error::Error for OrderMutationSendError {}
#[derive(Debug)]
pub(crate) struct OrderMutationResponse {
    pub client_message_id: String,
    pub payload_type: u32,
    pub payload: Value,
    pub receive_mono_ns: u64,
    pub dispatch_start_mono_ns: u64,
    pub dispatch_completed_mono_ns: u64,
    pub epoch_healthy: bool,
}
#[derive(Debug)]
pub(crate) enum OrderMutationReply {
    Execution(OrderMutationResponse),
    // A matched wire error; its code alone is not a proof of no execution effect.
    RequestRejected(OrderMutationResponse),
}
pub(crate) struct OrderMutationTicket {
    inner: Arc<Inner>,
    client_message_id: Option<String>,
    receiver: Option<oneshot::Receiver<Result<ReceivedResponse>>>,
    expected_account_id: u64,
    dispatch_start_mono_ns: u64,
    dispatch_completed_mono_ns: u64,
}
impl Drop for OrderMutationTicket {
    fn drop(&mut self) {
        if let Some(id) = self.client_message_id.take() {
            let inner = self.inner.clone();
            self.inner.runtime.spawn(async move {
                inner.pending.lock().await.remove(&id);
            });
        }
    }
}
impl OrderMutationTicket {
    pub(crate) fn client_message_id(&self) -> Result<&str> {
        self.client_message_id
            .as_deref()
            .context("Order mutation ticket already consumed")
    }
    /// The caller persists the actual OpenAPI envelope receipt before awaiting this response.
    pub(crate) async fn response(mut self) -> Result<OrderMutationReply> {
        let receiver = self
            .receiver
            .take()
            .context("Order mutation reply already consumed")?;
        let response = timeout(self.inner.request_timeout, receiver)
            .await
            .context("Order mutation response timed out; outcome unknown, never resend")?
            .context("Order mutation response channel ended; outcome unknown")??;
        let id = self
            .client_message_id
            .take()
            .context("Missing order correlation identity")?;
        self.inner.pending.lock().await.remove(&id);
        if let Some(account) = response.payload.get("ctidTraderAccountId") {
            ensure!(
                wire_u64(account) == Some(self.expected_account_id),
                "Order mutation response account mismatch"
            );
        }
        if matches!(response.payload_type, 2126 | 2132) {
            ensure!(
                response
                    .payload
                    .get("ctidTraderAccountId")
                    .and_then(wire_u64)
                    == Some(self.expected_account_id),
                "Order mutation execution/error lacks actual authenticated account identity"
            );
        }
        let response = OrderMutationResponse {
            client_message_id: id,
            payload_type: response.payload_type,
            payload: response.payload,
            receive_mono_ns: response.receive_mono_ns,
            dispatch_start_mono_ns: self.dispatch_start_mono_ns,
            dispatch_completed_mono_ns: self.dispatch_completed_mono_ns,
            epoch_healthy: self.inner.check_healthy().await.is_ok(),
        };
        match response.payload_type {
            2126 => Ok(OrderMutationReply::Execution(response)),
            50 | 2132 | 2142 => Ok(OrderMutationReply::RequestRejected(response)),
            _ => anyhow::bail!("Unclassified order response; retain unknown durable intent"),
        }
    }
}
impl OpenApiClient {
    /// Require the exact authenticated account and the broker's TRADE scope.
    pub(crate) fn require_order_trading_scope(&self, expected_account_id: u64) -> Result<()> {
        ensure!(
            valid_number(expected_account_id)
                && self.inner.account_id.load(Ordering::Acquire) == expected_account_id
                && self.inner.active.load(Ordering::Acquire)
                && self.inner.trading_scope.load(Ordering::Acquire),
            "Order mutation requires the authenticated account and TRADE scope"
        );
        Ok(())
    }
    pub(crate) async fn begin_market_order(
        &self,
        request: MarketOrderRequest,
        admission: OpenApiAdmission,
    ) -> std::result::Result<OrderMutationTicket, OrderMutationSendError> {
        request.validate().map_err(|_| OrderMutationSendError {
            may_have_been_sent: false,
            reason: "Invalid typed market order request",
        })?;
        let mut payload = json!({"ctidTraderAccountId":request.expected_account_id,
            "symbolId":request.symbol_id,"orderType":1,"tradeSide":request.side,
            "volume":request.volume,"timeInForce":3,"clientOrderId":request.client_order_id});
        if let Some(id) = request.position_id {
            payload["positionId"] = json!(id);
        }
        self.begin_order_mutation(
            OrderFrame {
                expected_account_id: request.expected_account_id,
                expected_revision: request.expected_revision,
                client_message_id: request.client_message_id,
                payload_type: 2106,
                payload,
            },
            admission,
        )
        .await
    }
    pub(crate) async fn begin_close_position(
        &self,
        request: PositionCloseRequest,
        admission: OpenApiAdmission,
    ) -> std::result::Result<OrderMutationTicket, OrderMutationSendError> {
        if !valid_number(request.expected_account_id)
            || !valid_number(request.position_id)
            || request.volume <= 0
            || !valid_id(&request.client_message_id)
        {
            return Err(OrderMutationSendError {
                may_have_been_sent: false,
                reason: "Invalid typed position close request",
            });
        }
        let payload = json!({"ctidTraderAccountId":request.expected_account_id,"positionId":request.position_id,"volume":request.volume});
        self.begin_order_mutation(
            OrderFrame {
                expected_account_id: request.expected_account_id,
                expected_revision: request.expected_revision,
                client_message_id: request.client_message_id,
                payload_type: 2111,
                payload,
            },
            admission,
        )
        .await
    }
    pub(crate) async fn begin_cancel_order(
        &self,
        request: CancelOrderRequest,
        admission: OpenApiAdmission,
    ) -> std::result::Result<OrderMutationTicket, OrderMutationSendError> {
        request.validate().map_err(|_| OrderMutationSendError {
            may_have_been_sent: false,
            reason: "Invalid typed cancel order request",
        })?;
        let payload =
            json!({"ctidTraderAccountId":request.expected_account_id,"orderId":request.order_id});
        self.begin_order_mutation(
            OrderFrame {
                expected_account_id: request.expected_account_id,
                expected_revision: request.expected_revision,
                client_message_id: request.client_message_id,
                payload_type: 2108,
                payload,
            },
            admission,
        )
        .await
    }
    async fn begin_order_mutation(
        &self,
        request: OrderFrame,
        admission: OpenApiAdmission,
    ) -> std::result::Result<OrderMutationTicket, OrderMutationSendError> {
        let not_sent = |reason| OrderMutationSendError {
            may_have_been_sent: false,
            reason,
        };
        self.require_order_trading_scope(request.expected_account_id)
            .map_err(|_| {
                not_sent("Order mutation lacks authenticated account or TRADE permission")
            })?;
        admission
            .check()
            .map_err(|_| not_sent("Order mutation account/token admission fenced"))?;
        self.require_order_trading_scope(request.expected_account_id)
            .map_err(|_| not_sent("Order mutation account permission changed before dispatch"))?;
        admission.check().map_err(|_| {
            not_sent("Order mutation account/token admission fenced before dispatch")
        })?;
        let mut writer = timeout(self.inner.request_timeout, self.inner.writer.lock())
            .await
            .map_err(|_| not_sent("Order mutation writer queue wait expired before dispatch"))?;
        self.require_order_trading_scope(request.expected_account_id)
            .map_err(|_| not_sent("Order mutation account permission changed in writer queue"))?;
        admission
            .check()
            .map_err(|_| not_sent("Order mutation admission fenced in writer queue"))?;
        let start = posix_monotonic_ns()
            .map_err(|_| not_sent("Order mutation monotonic clock unavailable"))?;
        if self.inner.account_revision.load(Ordering::Acquire) != request.expected_revision {
            return Err(not_sent("Account changed before order mutation dispatch"));
        }
        let (sender, receiver) = oneshot::channel();
        let mut pending = self.inner.pending.lock().await;
        if pending.len() >= self.inner.max_in_flight
            || pending.contains_key(&request.client_message_id)
        {
            return Err(not_sent(
                "Order mutation pending capacity or duplicate envelope identity",
            ));
        }
        pending.insert(
            request.client_message_id.clone(),
            Pending {
                expected_type: 2126,
                response: sender,
                deadline: Instant::now() + self.inner.request_timeout,
            },
        );
        drop(pending);
        let mut frame = Some(Message::Text(
            json!({"clientMsgId":request.client_message_id,
            "payloadType":request.payload_type,"payload":request.payload})
            .to_string()
            .into(),
        ));
        let mut started = false;
        let sent = timeout(
            self.inner.request_timeout,
            poll_fn(|cx| {
                // Every resumed sink poll is fenced. Once start_send is attempted, a failure
                // is conservatively unknown even if the transport buffered all bytes locally.
                if admission.check().is_err() || !self.inner.active.load(Ordering::Acquire) {
                    return Poll::Ready(Err(OrderMutationSendError {
                        may_have_been_sent: started,
                        reason: "Order mutation admission revoked while sending",
                    }));
                }
                if !started {
                    if self.inner.account_revision.load(Ordering::Acquire)
                        != request.expected_revision
                    {
                        return Poll::Ready(Err(not_sent(
                            "Account changed before order mutation first send",
                        )));
                    }
                    match Pin::new(&mut *writer).poll_ready(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(_)) => {
                            return Poll::Ready(Err(not_sent("Order mutation sink was not ready")));
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    if admission.check().is_err()
                        || !self.inner.active.load(Ordering::Acquire)
                        || self.inner.account_revision.load(Ordering::Acquire)
                            != request.expected_revision
                    {
                        return Poll::Ready(Err(not_sent(
                            "Order mutation admission revoked before start_send",
                        )));
                    }
                    started = true;
                    if Pin::new(&mut *writer)
                        .start_send(frame.take().expect("single typed order frame"))
                        .is_err()
                    {
                        return Poll::Ready(Err(OrderMutationSendError {
                            may_have_been_sent: true,
                            reason: "Order mutation start_send failed; outcome unknown",
                        }));
                    }
                }
                match Pin::new(&mut *writer).poll_flush(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                    Poll::Ready(Err(_)) => Poll::Ready(Err(OrderMutationSendError {
                        may_have_been_sent: true,
                        reason: "Order mutation flush failed; outcome unknown",
                    })),
                }
            }),
        )
        .await;
        drop(writer);
        let sent = sent.unwrap_or(Err(OrderMutationSendError {
            may_have_been_sent: started,
            reason: "Order mutation send deadline; do not replay",
        }));
        if let Err(error) = sent {
            self.inner
                .pending
                .lock()
                .await
                .remove(&request.client_message_id);
            self.inner
                .fail("order mutation writer failed or was fenced")
                .await;
            return Err(error);
        }
        let completed = posix_monotonic_ns().map_err(|_| OrderMutationSendError {
            may_have_been_sent: true,
            reason: "Order mutation dispatch clock unavailable after send",
        })?;
        Ok(OrderMutationTicket {
            inner: self.inner.clone(),
            client_message_id: Some(request.client_message_id),
            receiver: Some(receiver),
            expected_account_id: request.expected_account_id,
            dispatch_start_mono_ns: start,
            dispatch_completed_mono_ns: completed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    struct Peer {
        client: OpenApiClient,
        sent: mpsc::Receiver<Value>,
        inject: mpsc::Sender<Value>,
        activity: Arc<AtomicBool>,
    }
    impl Peer {
        fn admission(&self) -> OpenApiAdmission {
            OpenApiAdmission::new(self.activity.clone(), None)
        }
        fn request(&self, id: &str) -> MarketOrderRequest {
            MarketOrderRequest {
                expected_account_id: 42,
                expected_revision: self.client.account_revision(),
                symbol_id: 7,
                side: 1,
                position_id: None,
                volume: 1,
                client_order_id: id.into(),
                client_message_id: id.into(),
            }
        }
    }
    async fn mock_peer(scope: u32) -> Peer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (sent_tx, sent) = mpsc::channel(16);
        let (inject, mut incoming) = mpsc::channel::<Value>(16);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            loop {
                tokio::select! {
                    frame=socket.next()=>{
                        let Some(Ok(Message::Text(text)))=frame else {break};
                        let req:Value=serde_json::from_str(&text).unwrap();
                        let (kind,payload)=match req["payloadType"].as_u64().unwrap() {
                            51=>continue,
                            2100=>(2101,json!({})),
                            2149=>(2150,json!({"accessToken":req["payload"]["accessToken"],"permissionScope":scope,"ctidTraderAccount":[{"ctidTraderAccountId":"42","traderLogin":"100001","isLive":false}]})),
                            2102=>(2103,json!({"ctidTraderAccountId":"42"})),
                            2121=>(2122,json!({"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42","traderLogin":"100001","depositAssetId":"1","balance":"100","moneyDigits":2}})),
                            2106 | 2108=>{sent_tx.send(req).await.unwrap();continue;},
                            _=>panic!("Unexpected close fixture request"),
                        };
                        if socket.send(Message::Text(json!({"clientMsgId":req["clientMsgId"],"payloadType":kind,"payload":payload}).to_string().into())).await.is_err(){break;}
                    }
                    event=incoming.recv()=>{
                        let Some(event)=event else {break};
                        if socket.send(Message::Text(event.to_string().into())).await.is_err(){break;}
                    }
                }
            }
        });
        let mut config = OpenApiConfig::demo(100_001);
        config.request_timeout = Duration::from_millis(250);
        let client = OpenApiClient::connect_to(
            &endpoint,
            config,
            OpenApiCredentials {
                client_id: "synthetic-id".into(),
                client_secret: "synthetic-secret".into(),
                access_token: "synthetic-token".into(),
            },
        )
        .await
        .unwrap();
        Peer {
            client,
            sent,
            inject,
            activity: Arc::new(AtomicBool::new(true)),
        }
    }
    fn event(id: &str, kind: u32, payload: &Value) -> Value {
        json!({"clientMsgId":id,"payloadType":kind,"payload":payload})
    }
    #[tokio::test]
    async fn view_scope_and_invalid_order_never_write() {
        let mut peer = mock_peer(0).await;
        let error = match peer
            .client
            .begin_market_order(peer.request("view"), peer.admission())
            .await
        {
            Ok(_) => panic!("View token must not close"),
            Err(error) => error,
        };
        assert!(!error.may_have_been_sent);
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
        let mut peer = mock_peer(1).await;
        let mut request = peer.request("invalid");
        request.volume = 0;
        assert!(
            peer.client
                .begin_market_order(request, peer.admission())
                .await
                .is_err()
        );
        let mut request = peer.request("wrong-account");
        request.expected_account_id = 43;
        assert!(
            peer.client
                .begin_market_order(request, peer.admission())
                .await
                .is_err()
        );
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
    }
    #[tokio::test]
    async fn correlated_execution_is_fanned_out_before_reply_and_later_fill_is_not_lost() {
        let mut peer = mock_peer(1).await;
        let mut one = peer.client.take_events().unwrap();
        let mut two = peer.client.subscribe_events().unwrap();
        let ticket = peer
            .client
            .begin_market_order(peer.request("own-close"), peer.admission())
            .await
            .unwrap();
        let wire = peer.sent.recv().await.unwrap();
        assert_eq!(
            wire,
            json!({"clientMsgId":"own-close","payloadType":2106,"payload":{"ctidTraderAccountId":42,"symbolId":7,"orderType":1,"tradeSide":1,"volume":1,"timeInForce":3,"clientOrderId":"own-close"}})
        );
        assert_eq!(ticket.client_message_id().unwrap(), "own-close");
        peer.inject
            .send(event(
                "own-close",
                2126,
                &json!({"ctidTraderAccountId":"42","executionType":2}),
            ))
            .await
            .unwrap();
        let reply = ticket.response().await.unwrap();
        assert!(matches!(reply, OrderMutationReply::Execution(_)));
        assert_eq!(peer.client.account_revision(), 1);
        assert_eq!(one.try_next().unwrap().unwrap().payload_type, 2126);
        assert_eq!(two.try_next().unwrap().unwrap().payload_type, 2126);
        peer.inject
            .send(event(
                "own-close",
                2126,
                &json!({"ctidTraderAccountId":"42","executionType":3}),
            ))
            .await
            .unwrap();
        assert_eq!(one.next().await.unwrap().payload["executionType"], 3);
        assert_eq!(two.next().await.unwrap().payload["executionType"], 3);
        assert_eq!(peer.client.account_revision(), 2);
        assert!(peer.client.is_connected().await);
        peer.client.close().await;
    }
    #[tokio::test]
    async fn timeout_preserves_late_account_event_without_fabricating_a_reply() {
        let mut peer = mock_peer(1).await;
        let mut events = peer.client.take_events().unwrap();
        let ticket = peer
            .client
            .begin_market_order(peer.request("late"), peer.admission())
            .await
            .unwrap();
        peer.sent.recv().await.unwrap();
        assert!(ticket.response().await.is_err());
        peer.inject
            .send(event(
                "late",
                2126,
                &json!({"ctidTraderAccountId":"42","executionType":3}),
            ))
            .await
            .unwrap();
        assert_eq!(events.next().await.unwrap().payload_type, 2126);
        assert_eq!(peer.client.account_revision(), 1);
        assert!(peer.client.is_connected().await);
        peer.client.close().await;
    }
    #[tokio::test]
    async fn typed_request_error_preserves_actual_payload_and_envelope() {
        for kind in [50, 2132, 2142] {
            let mut peer = mock_peer(1).await;
            let mut events = peer.client.take_events().unwrap();
            let ticket = peer
                .client
                .begin_market_order(peer.request("reject"), peer.admission())
                .await
                .unwrap();
            peer.sent.recv().await.unwrap();
            let raw = if kind == 50 {
                json!({"errorCode":"TIMEOUT_ERROR"})
            } else {
                json!({"ctidTraderAccountId":"42","errorCode":"POSITION_NOT_OPEN"})
            };
            peer.inject.send(event("reject", kind, &raw)).await.unwrap();
            let OrderMutationReply::RequestRejected(response) = ticket.response().await.unwrap()
            else {
                panic!("Request error is not an execution")
            };
            assert_eq!(response.payload, raw);
            assert_eq!(response.client_message_id, "reject");
            assert_eq!(response.payload_type, kind);
            assert!(response.receive_mono_ns >= response.dispatch_start_mono_ns);
            assert_eq!(events.try_next().unwrap().unwrap().payload, raw);
            peer.client.close().await;
        }
    }
    #[tokio::test]
    async fn caller_burst_is_not_delayed_by_a_private_per_second_quota() {
        let peer = mock_peer(1).await;
        for _ in 0..60 {
            peer.client.request(ReadOnlyRequest::Trader, json!({})).await.unwrap();
        }
        peer.client.close().await;
    }
    #[tokio::test]
    async fn writer_queue_revocation_proves_no_close_sent() {
        let mut peer = mock_peer(1).await;
        let writer = peer.client.inner.writer.lock().await;
        let client = peer.client.clone();
        let guard = peer.admission();
        let request = peer.request("fenced");
        let send = client.begin_market_order(request, guard);
        tokio::pin!(send);
        assert!(futures_util::poll!(send.as_mut()).is_pending());
        // No local pacing gate: the writer held above is the only pending write boundary.
        peer.activity.store(false, Ordering::Release);
        drop(writer);
        let error = match send.await {
            Ok(_) => panic!("Revoked request wrote"),
            Err(error) => error,
        };
        assert!(!error.may_have_been_sent);
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
    }
    #[tokio::test]
    async fn account_change_in_writer_queue_proves_no_stale_close_sent() {
        let mut peer = mock_peer(1).await;
        let mut events = peer.client.take_events().unwrap();
        let writer = peer.client.inner.writer.lock().await;
        let client = peer.client.clone();
        let guard = peer.admission();
        let request = peer.request("fenced");
        let send = client.begin_market_order(request, guard);
        tokio::pin!(send);
        assert!(futures_util::poll!(send.as_mut()).is_pending());
        // No local pacing gate: the writer held above is the only pending write boundary.
        peer.inject
            .send(event(
                "external-mutation",
                2126,
                &json!({"ctidTraderAccountId":"42"}),
            ))
            .await
            .unwrap();
        events.next().await.unwrap();
        assert_eq!(peer.client.account_revision(), 1);
        drop(writer);
        let error = match send.await {
            Ok(_) => panic!("Revoked request wrote"),
            Err(error) => error,
        };
        assert!(!error.may_have_been_sent);
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
    }
    #[tokio::test]
    async fn mismatched_account_execution_cannot_complete_close() {
        let mut peer = mock_peer(1).await;
        let ticket = peer
            .client
            .begin_market_order(peer.request("wrong-account"), peer.admission())
            .await
            .unwrap();
        peer.sent.recv().await.unwrap();
        peer.inject
            .send(event(
                "wrong-account",
                2126,
                &json!({"ctidTraderAccountId":"43"}),
            ))
            .await
            .unwrap();
        assert!(ticket.response().await.is_err());
        assert!(!peer.client.is_connected().await);
    }
    #[tokio::test]
    async fn foreign_envelope_does_not_steal_the_correlated_close_reply() {
        let mut peer = mock_peer(1).await;
        let mut events = peer.client.take_events().unwrap();
        let mut ticket = peer
            .client
            .begin_market_order(peer.request("ours"), peer.admission())
            .await
            .unwrap();
        peer.sent.recv().await.unwrap();
        peer.inject
            .send(event("foreign", 2126, &json!({"ctidTraderAccountId":"42"})))
            .await
            .unwrap();
        events.next().await.unwrap();
        assert!(matches!(
            ticket.receiver.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        peer.inject
            .send(event("ours", 2126, &json!({"ctidTraderAccountId":"42"})))
            .await
            .unwrap();
        assert!(matches!(
            ticket.response().await.unwrap(),
            OrderMutationReply::Execution(_)
        ));
        peer.client.close().await;
    }
    #[tokio::test]
    async fn order_error_requires_actual_account_identity() {
        for raw in [
            json!({"errorCode":"POSITION_NOT_OPEN"}),
            json!({"ctidTraderAccountId":"43","errorCode":"POSITION_NOT_OPEN"}),
        ] {
            let mut peer = mock_peer(1).await;
            let ticket = peer
                .client
                .begin_market_order(peer.request("rejected"), peer.admission())
                .await
                .unwrap();
            peer.sent.recv().await.unwrap();
            peer.inject
                .send(event("rejected", 2132, &raw))
                .await
                .unwrap();
            assert!(ticket.response().await.is_err());
            assert!(!peer.client.is_connected().await);
        }
    }
    #[tokio::test]
    async fn cancel_uses_only_bound_order_identity_and_correlates_reply() {
        let mut peer = mock_peer(1).await;
        let request = CancelOrderRequest {
            expected_account_id: 42,
            expected_revision: peer.client.account_revision(),
            order_id: 789,
            client_message_id: "cancel-1".into(),
        };
        let ticket = peer
            .client
            .begin_cancel_order(request, peer.admission())
            .await
            .unwrap();
        assert_eq!(
            peer.sent.recv().await.unwrap(),
            json!({"clientMsgId":"cancel-1","payloadType":2108,"payload":{"ctidTraderAccountId":42,"orderId":789}})
        );
        peer.inject
            .send(event(
                "cancel-1",
                2126,
                &json!({"ctidTraderAccountId":"42","executionType":5}),
            ))
            .await
            .unwrap();
        assert!(matches!(
            ticket.response().await.unwrap(),
            OrderMutationReply::Execution(_)
        ));
        let invalid = CancelOrderRequest {
            expected_account_id: 42,
            expected_revision: peer.client.account_revision(),
            order_id: 0,
            client_message_id: "bad".into(),
        };
        let err = match peer
            .client
            .begin_cancel_order(invalid, peer.admission())
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("invalid cancel was sent"),
        };
        assert!(!err.may_have_been_sent);
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
    }
    #[tokio::test]
    async fn cancel_view_wrong_account_and_stale_revision_never_write() {
        let mut view = mock_peer(0).await;
        let cancel = CancelOrderRequest {
            expected_account_id: 42,
            expected_revision: view.client.account_revision(),
            order_id: 9,
            client_message_id: "c".into(),
        };
        let error = match view
            .client
            .begin_cancel_order(cancel, view.admission())
            .await
        {
            Err(e) => e,
            Ok(_) => panic!("view scope wrote"),
        };
        assert!(!error.may_have_been_sent);
        assert!(view.sent.try_recv().is_err());
        view.client.close().await;
        let mut peer = mock_peer(1).await;
        for (account, revision) in [
            (43, peer.client.account_revision()),
            (42, peer.client.account_revision() + 1),
        ] {
            let cancel = CancelOrderRequest {
                expected_account_id: account,
                expected_revision: revision,
                order_id: 9,
                client_message_id: "c".into(),
            };
            let error = match peer
                .client
                .begin_cancel_order(cancel, peer.admission())
                .await
            {
                Err(e) => e,
                Ok(_) => panic!("invalid cancellation wrote"),
            };
            assert!(!error.may_have_been_sent);
        }
        assert!(peer.sent.try_recv().is_err());
        peer.client.close().await;
    }
    #[tokio::test]
    async fn market_position_is_optional_and_equal_client_ids_are_allowed() {
        let mut peer = mock_peer(1).await;
        let mut request = peer.request("SAME");
        request.position_id = Some(77);
        let ticket = peer
            .client
            .begin_market_order(request, peer.admission())
            .await
            .unwrap();
        let wire = peer.sent.recv().await.unwrap();
        assert_eq!(wire["clientMsgId"], "SAME");
        assert_eq!(wire["payload"]["clientOrderId"], "SAME");
        assert_eq!(wire["payload"]["positionId"], 77);
        drop(ticket);
        peer.client.close().await;
    }
}
