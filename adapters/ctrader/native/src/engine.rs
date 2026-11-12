//! Framework-free account ownership, durable mutations and exact recovery evidence.
use crate::execution::{raw_account, raw_observations};
use crate::openapi::funds::{AccountFunds, ExpectedFundsAccount, FundsWindow};
use crate::openapi::model::{BrokerDeal, BrokerOrder, BrokerPosition, BrokerTrader};
use crate::{clock::monotonic_ns, execution::inflight::*, instruments::CfdSymbolSpec, openapi::*};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct FundsRevisionChanged;
impl std::fmt::Display for FundsRevisionChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Reader account revision changed during funds collection")
    }
}
impl std::error::Error for FundsRevisionChanged {}

pub struct Connection {
    pub lease: OpenApiLease,
    pub events: Mutex<OpenApiEvents>,
    pub requests: Option<InFlightRequests>,
    pub bound: usize,
    carrier: Option<raw_account::Carrier>,
    originals: std::sync::Mutex<raw_observations::PendingExecutions>,
    metadata_task: Option<tokio::task::JoinHandle<()>>,
    ready: AtomicBool,
    live: bool,
    close_bindings: std::sync::Mutex<HashMap<u64, String>>,
    metadata: std::sync::Mutex<HashMap<u64, Value>>,
    lights: std::sync::Mutex<Value>,
    assets: std::sync::Mutex<Value>,
}
fn number(value: &Value, name: &str) -> Result<u64> {
    value[name]
        .as_u64()
        .or_else(|| value[name].as_str()?.parse().ok())
        .context(format!("Missing integer {name}"))
}
fn rows<'a>(value: &'a Value, name: &str) -> Result<&'a [Value]> {
    match value.get(name) {
        None => Ok(&[]),
        Some(Value::Array(rows)) => Ok(rows),
        _ => bail!("Invalid broker repeated field {name}"),
    }
}
pub fn wall_ns() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos()
        .try_into()?)
}
pub fn now_ms() -> Result<i64> {
    Ok((wall_ns()? / 1_000_000).try_into()?)
}
impl Connection {
    pub async fn acquire(
        session: Arc<OpenApiAccountSession>,
        execution: bool,
        bound: usize,
    ) -> Result<Self> {
        let lease = session.acquire().await?;
        Self::from_lease(
            lease,
            session.config().environment == OpenApiEnvironment::Live,
            execution,
            bound,
        )
        .await
    }
    async fn from_lease(
        mut lease: OpenApiLease,
        live: bool,
        execution: bool,
        bound: usize,
    ) -> Result<Self> {
        let events = Mutex::new(lease.take_events()?);
        let mut carrier = None;
        let mut metadata_task = None;
        let requests = if execution {
            let trader = lease.client().account_snapshot().await?;
            lease
                .client()
                .require_order_trading_scope(trader.ctid_trader_account_id)?;
            Some(InFlightRequests::new(trader.ctid_trader_account_id, bound)?)
        } else {
            None
        };
        if requests.is_some() {
            let trader = lease.client().account_snapshot().await?;
            let capture = raw_account::Carrier::new(lease.epoch(), trader.ctid_trader_account_id);
            let worker = capture.clone();
            let api = lease.client().clone();
            let deposit = trader.deposit_asset_id;
            metadata_task = Some(tokio::spawn(async move { worker.run(api, deposit).await }));
            carrier = Some(capture);
        }
        let close_bindings = HashMap::new();
        Ok(Self {
            close_bindings: std::sync::Mutex::new(close_bindings),
            lease,
            events,
            requests,
            bound,
            carrier,
            metadata_task,
            originals: std::sync::Mutex::new(raw_observations::PendingExecutions::default()),
            ready: AtomicBool::new(false),
            live,
            metadata: std::sync::Mutex::new(HashMap::new()),
            lights: std::sync::Mutex::new(Value::Null),
            assets: std::sync::Mutex::new(Value::Null),
        })
    }
    pub async fn request(&self, operation: ReadOnlyRequest, payload: Value) -> Result<Value> {
        self.lease.admission_guard().check()?;
        let value = self.lease.client().request(operation, payload).await?;
        match operation {
            ReadOnlyRequest::Symbols => {
                *self
                    .lights
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Metadata poisoned"))? =
                    value.get("symbol").context("Missing symbols")?.clone()
            }
            ReadOnlyRequest::Assets => {
                *self
                    .assets
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Metadata poisoned"))? =
                    value.get("asset").context("Missing assets")?.clone()
            }
            ReadOnlyRequest::SymbolDetails => {
                let mut cache = self
                    .metadata
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Metadata poisoned"))?;
                for row in value["symbol"]
                    .as_array()
                    .context("Missing symbol details")?
                {
                    cache.insert(number(row, "symbolId")?, row.clone());
                }
            }
            _ => {}
        }
        Ok(value)
    }
    /// Complete bounded history: split truncated windows, never pretend the first page is complete.
    pub async fn history(
        &self,
        operation: ReadOnlyRequest,
        from: i64,
        to: i64,
    ) -> Result<Vec<Value>> {
        ensure!(from >= 0 && to >= from, "Invalid history bounds");
        let (field, identity) = match operation {
            ReadOnlyRequest::Orders => ("order", "orderId"),
            ReadOnlyRequest::Deals => ("deal", "dealId"),
            _ => bail!("Not a supported history operation"),
        };
        let mut windows = vec![(from, to)];
        let mut result = HashMap::new();
        let mut visits = 0;
        while let Some((from, to)) = windows.pop() {
            visits += 1;
            ensure!(visits <= self.bound, "History request bound exceeded");
            let value = self
                .request(operation, json!({"fromTimestamp":from,"toTimestamp":to}))
                .await?;
            if value["hasMore"]
                .as_bool()
                .context("Missing history completeness marker")?
            {
                ensure!(from < to, "Truncated history in indivisible millisecond");
                let mid = from + (to - from) / 2;
                windows.push((from, mid));
                windows.push((mid + 1, to));
                continue;
            }
            for row in rows(&value, field)? {
                if operation == ReadOnlyRequest::Orders {
                    BrokerOrder::from_wire(row)?;
                } else {
                    BrokerDeal::from_wire(row)?;
                }
                let id = number(row, identity)?;
                ensure!(id > 0, "Zero history identity");
                if let Some(old) = result.insert(id, row.clone()) {
                    ensure!(old == *row, "Conflicting history duplicate");
                }
                ensure!(result.len() <= self.bound, "History exceeds bound");
            }
        }
        let mut rows = result.into_iter().collect::<Vec<_>>();
        rows.sort_by_key(|(id, _)| *id);
        Ok(rows.into_iter().map(|(_, v)| v).collect())
    }
    /// Standard venue snapshot for Na reconciliation; no private requests replay or identity adoption.
    pub async fn snapshot(&self, lookback_ms: i64, require_flat: bool) -> Result<Value> {
        let to = now_ms()?;
        let from = to.saturating_sub(lookback_ms).max(0);
        let trader = self.request(ReadOnlyRequest::Trader, json!({})).await?;
        let parsed = BrokerTrader::from_wire(trader.get("trader").context("Missing Trader")?)?;
        ensure!(
            parsed.is_hedged_full_access(),
            "Hedged full-access account required"
        );
        let reconcile = self.request(ReadOnlyRequest::Reconcile, json!({})).await?;
        let positions = rows(&reconcile, "position")?;
        let pending_orders = rows(&reconcile, "order")?;
        ensure!(
            positions.len() <= self.bound && pending_orders.len() <= self.bound,
            "Reconcile exceeds bound"
        );
        for p in positions {
            BrokerPosition::from_wire(p)?;
        }
        if require_flat {
            ensure!(
                positions.is_empty() && pending_orders.is_empty(),
                "Account must be flat on connect"
            );
        }
        let mut orders = self.history(ReadOnlyRequest::Orders, from, to).await?;
        for order in pending_orders {
            BrokerOrder::from_wire(order)?;
            if !orders.iter().any(|r| r["orderId"] == order["orderId"]) {
                orders.push(order.clone());
            }
        }
        let deals = self.history(ReadOnlyRequest::Deals, from, to).await?;
        self.ready.store(true, Ordering::Release);
        Ok(
            json!({"epoch":self.lease.epoch(),"revision":self.lease.client().account_revision(),"trader":trader,
            "reconcile":reconcile,"orders":orders,"deals":deals,"from_ms":from,"to_ms":to,"close_bindings":self.close_bindings.lock().map_err(|_|anyhow::anyhow!("Close mapping poisoned"))?.clone()}),
        )
    }
    pub async fn funds(&self) -> Result<Value> {
        let api = self.lease.client();
        ensure!(api.is_connected().await, "Disconnected funds source");
        if let Some(carrier) = &self.carrier {
            // The preflight only selects raw evidence. Authoritative funds below
            // are collected afresh after preparation, with the original fences.
            let needed = {
                let inventory = self.request(ReadOnlyRequest::Reconcile, json!({})).await?;
                raw_account::required(&inventory)?
            };
            // Evidence failure remains explicit in capture; valid balances are
            // never fabricated or withheld merely for optional valuation data.
            let _ = carrier.prepare_evidence(needed).await;
        }
        let revision = api.account_revision();
        let start = monotonic_ns()?;
        let started_utc = wall_ns()?;
        let before = self.request(ReadOnlyRequest::Trader, json!({})).await?;
        let trader =
            BrokerTrader::from_wire(before.get("trader").context("Missing funds Trader")?)?;
        let reconcile_before = self.request(ReadOnlyRequest::Reconcile, json!({})).await?;
        let pnl = self
            .request(ReadOnlyRequest::PositionPnL, json!({}))
            .await?;
        let reconcile_after = self.request(ReadOnlyRequest::Reconcile, json!({})).await?;
        let after = self.request(ReadOnlyRequest::Trader, json!({})).await?;
        let assets = self.request(ReadOnlyRequest::Assets, json!({})).await?;
        let end = monotonic_ns()?;
        ensure!(api.is_connected().await, "Disconnected funds observation");
        let account = ExpectedFundsAccount {
            ctid_trader_account_id: trader.account.ctid_trader_account_id,
            trader_login: trader.account.trader_login,
            deposit_asset_id: trader.account.deposit_asset_id,
        };
        if revision != api.account_revision() {
            return Err(FundsRevisionChanged.into());
        }
        let window = FundsWindow {
            epoch_before: self.lease.epoch(),
            epoch_after: self.lease.epoch(),
            revision_before: revision,
            revision_after: api.account_revision(),
            started_mono_ns: start,
            completed_mono_ns: end,
            now_mono_ns: end,
            max_span_ns: 2_000_000_000,
            max_age_ns: 3_000_000_000,
            completed_utc_ns: wall_ns()?,
        };
        let funds = AccountFunds::from_wire(
            &account,
            &window,
            &before,
            &reconcile_before,
            &pnl,
            &reconcile_after,
            &after,
            &assets,
        )?;
        let funds_json = funds.to_json();
        let raw_snapshot = self.carrier.as_ref().map(|carrier| {
            carrier.capture(
                api,
                &account,
                funds_json["currency"].as_str().unwrap_or(""),
                self.live,
                &window,
                started_utc,
                &assets,
                [&before, &reconcile_before, &pnl, &reconcile_after, &after],
            )
        });
        let batch = self
            .originals
            .lock()
            .map_err(|_| anyhow::anyhow!("Original execution state poisoned"))?
            .batch();
        Ok(
            json!({"funds":funds_json,"ctrader_raw_snapshot":raw_snapshot,
            "execution_batch":batch.map(|b|json!({"through":b.through,"events":b.events})),
            "raw":{"trader_before":before,"reconcile_before":reconcile_before,
            "pnl":pnl,"reconcile_after":reconcile_after,"trader_after":after,"assets":assets}}),
        )
    }
    async fn persist_order(&self, payload: Value, correlated: Option<&str>) -> Result<()> {
        let Some(requests) = &self.requests else {
            return Ok(());
        };
        let Some(raw) = payload.get("order").filter(|v| !v.is_null()) else {
            return Ok(());
        };
        let order = BrokerOrder::from_wire(raw)?;
        let account = number(&payload, "ctidTraderAccountId")?;
        for pending in requests.pending().await? {
            let matches = match &pending.intent.command {
                IntentCommand::OpenApiMarket {
                    native_account_id,
                    symbol_id,
                    side,
                    quantity,
                    position_id,
                    ..
                } => {
                    if order.client_order_id.as_deref() != Some(&pending.intent.client_order_id) {
                        continue;
                    }
                    ensure!(
                        *native_account_id == account
                            && order.symbol_id == *symbol_id
                            && order.trade_side == *side
                            && order.quantity == quantity.parse::<rust_decimal::Decimal>()?
                            && order.order_type == 1
                            && order.time_in_force == 3
                            && position_id.is_none_or(|id| order.position_id == Some(id)),
                        "Native order identity mismatch"
                    );
                    true
                }
                IntentCommand::OpenApiClose {
                    native_account_id,
                    symbol_id,
                    side,
                    quantity,
                    position_id,
                    ..
                } => {
                    if pending.venue_order_id != Some(order.order_id)
                        && correlated != Some(pending.intent.client_order_id.as_str())
                    {
                        continue;
                    }
                    ensure!(
                        *native_account_id == account
                            && order.symbol_id == *symbol_id
                            && order.trade_side == *side
                            && order.quantity == quantity.parse::<rust_decimal::Decimal>()?
                            && order.position_id.is_none_or(|id| id == *position_id)
                            && order.closing_order.is_none_or(|v| v),
                        "Native atomic close identity mismatch"
                    );
                    self.close_bindings
                        .lock()
                        .map_err(|_| anyhow::anyhow!("Close mapping poisoned"))?
                        .insert(order.order_id, pending.intent.client_order_id.clone());
                    true
                }
                IntentCommand::OpenApiCancel {
                    native_account_id,
                    venue_order_id,
                    original_client_order_id,
                    ..
                } => {
                    if order.order_id != *venue_order_id {
                        continue;
                    }
                    ensure!(
                        *native_account_id == account
                            && original_client_order_id
                                .as_ref()
                                .is_none_or(|v| order.client_order_id.as_ref() == Some(v)),
                        "Native cancel identity mismatch"
                    );
                    true
                }
                _ => false,
            };
            if !matches {
                continue;
            }
            requests
                .bind(&pending.intent.client_order_id, order.order_id)
                .await?;
            if (2..=5).contains(&order.order_status) {
                requests.resolve(&pending.intent.client_order_id,IntentResolution {kind:ResolutionKind::BrokerTerminal,
                    evidence:json!({"ctid_trader_account_id":account,"order_id":order.order_id,"order_status":order.order_status,
                        "symbol_id":order.symbol_id,"side":order.trade_side,"quantity":order.quantity.to_string(),
                        "filled_quantity":order.filled_quantity.to_string(),"position_id":order.position_id,"fees":null,"raw":payload})}).await?;
            }
        }
        let trim_needed = self
            .close_bindings
            .lock()
            .map_err(|_| anyhow::anyhow!("Close mapping poisoned"))?
            .len()
            > self.bound;
        if trim_needed {
            let pending = requests
                .pending()
                .await?
                .into_iter()
                .filter_map(|p| p.venue_order_id)
                .collect::<std::collections::HashSet<_>>();
            let mut mappings = self
                .close_bindings
                .lock()
                .map_err(|_| anyhow::anyhow!("Close mapping poisoned"))?;
            let mut removable = mappings
                .keys()
                .copied()
                .filter(|id| !pending.contains(id) && *id != order.order_id)
                .collect::<Vec<_>>();
            removable.sort_unstable();
            for id in removable
                .into_iter()
                .take(mappings.len().saturating_sub(self.bound))
            {
                mappings.remove(&id);
            }
            ensure!(
                mappings.len() <= self.bound,
                "Unresolved close mappings exceed bound"
            );
        }
        Ok(())
    }
    pub async fn next(&self) -> Result<Value> {
        loop {
            let event = self
                .events
                .lock()
                .await
                .next()
                .await
                .context("Open API event epoch ended")?;
            if let Some(carrier) = &self.carrier {
                if event.payload_type == 2131 {
                    raw_account::spot(
                        &carrier.shared,
                        &carrier.dropped,
                        self.lease.epoch(),
                        &event.payload,
                        event.received_mono_ns,
                    );
                }
            }
            // Python execution ignores non-execution frames. Keep original spot
            // handling above, and avoid JSON/PyO3/asyncio hops for discarded frames.
            if self.requests.is_some() && event.payload_type != 2126 {
                continue;
            }
            if event.payload_type == 2126 {
                if let Some(requests) = &self.requests {
                    let correlated = requests.correlate_event(&event).await?;
                    self.persist_order(event.payload.clone(), correlated.as_deref())
                        .await?;
                    let cid = event
                        .payload
                        .get("order")
                        .and_then(|o| number(o, "orderId").ok())
                        .and_then(|id| self.close_bindings.lock().ok()?.get(&id).cloned());
                    let binding = if let Some(cid) = cid {
                        requests.native_close_binding(&cid).await?
                    } else {
                        None
                    };
                    self.originals
                        .lock()
                        .map_err(|_| anyhow::anyhow!("Original execution state poisoned"))?
                        .record_bound(
                            &event,
                            self.lease.client().authenticated_account_id()?,
                            binding,
                        )?;
                }
            }
            return Ok(
                json!({"payload_type":event.payload_type,"payload":event.payload,"epoch":self.lease.epoch(),
            "account_revision":event.account_revision,"received_mono_ns":event.received_mono_ns,
            "received_wall_ns":u64::try_from(event.received_wall_time.duration_since(UNIX_EPOCH)?.as_nanos())?,
            "local_client_order_id":event.payload.get("order").and_then(|o|number(o,"orderId").ok()).and_then(|id|self.close_bindings.lock().ok()?.get(&id).cloned())}),
            );
        }
    }
    /// One owned task retains the in-flight request across caller cancellation; state is memory-only.
    pub async fn mutate(&self, command: Value) -> Result<Value> {
        let requests = self
            .requests
            .as_ref()
            .context("Read-only connection cannot mutate")?;
        ensure!(
            self.ready.load(Ordering::Acquire),
            "Execution client has not completed its initial venue snapshot"
        );
        let cid = command["client_order_id"]
            .as_str()
            .context("Missing CID")?
            .to_owned();
        let envelope = command["envelope_id"]
            .as_str()
            .context("Missing envelope CID")?
            .to_owned();
        let account = self.lease.client().authenticated_account_id()?;
        let revision = self.lease.client().account_revision();
        let kind = command["kind"].as_str().context("Missing mutation kind")?;
        let _request_lifetime;
        let intent = if kind == "market" || kind == "close" {
            let symbol_id = number(&command["light"], "symbolId")?;
            let detail = self
                .metadata
                .lock()
                .map_err(|_| anyhow::anyhow!("Metadata poisoned"))?
                .get(&symbol_id)
                .context("Unloaded broker contract")?
                .clone();
            let light = {
                let lights = self
                    .lights
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Metadata poisoned"))?;
                let rows = lights
                    .as_array()
                    .context("Symbols not loaded")?
                    .iter()
                    .filter(|r| number(r, "symbolId").ok() == Some(symbol_id))
                    .collect::<Vec<_>>();
                ensure!(
                    rows.len() == 1,
                    "Missing or ambiguous broker light identity"
                );
                rows[0].clone()
            };
            let assets = self
                .assets
                .lock()
                .map_err(|_| anyhow::anyhow!("Metadata poisoned"))?
                .clone();
            ensure!(
                light == command["light"]
                    && detail == command["detail"]
                    && assets == command["assets"],
                "Command metadata differs from authenticated broker contract"
            );
            let spec = CfdSymbolSpec::from_wire(&light, &detail, &assets)?;
            let quantity: rust_decimal::Decimal = command["quantity"]
                .as_str()
                .context("Exact quantity string required")?
                .parse()?;
            let volume = if kind == "close" {
                use rust_decimal::prelude::ToPrimitive;
                let wire = quantity
                    .checked_mul(rust_decimal::Decimal::from(100))
                    .context("Close volume overflow")?;
                ensure!(
                    quantity > rust_decimal::Decimal::ZERO && wire.fract().is_zero(),
                    "Close requires exact positive centi-units"
                );
                wire.to_i64().context("Close volume exceeds wire integer")?
            } else {
                spec.validate_quantity(quantity)?
            };
            let side = number(&command, "side")? as u32;
            let position = command
                .get("position_id")
                .filter(|v| !v.is_null())
                .map(|_| number(&command, "position_id"))
                .transpose()?;
            let mode = crate::openapi::model::enum_field(
                &detail,
                "tradingMode",
                &[
                    ("ENABLED", 0),
                    ("DISABLED_WITHOUT_PENDINGS_EXECUTION", 1),
                    ("DISABLED_WITH_PENDINGS_EXECUTION", 2),
                    ("CLOSE_ONLY_MODE", 3),
                ],
                Some(0),
            )?;
            ensure!(
                mode == 0 || (mode == 3 && position.is_some()),
                "Broker trading mode forbids this order"
            );
            ensure!(
                position.is_some()
                    || side != 2
                    || detail["enableShortSelling"].as_bool().unwrap_or(false),
                "Broker forbids opening shorts"
            );
            if kind == "close" {
                ensure!(
                    position.is_some(),
                    "Reduce-only requires explicit native position"
                );
            }
            if kind == "close" {
                let target = position.context("Missing close position")?;
                _request_lifetime = requests
                    .begin(OrderIntent {
                        client_order_id: cid.clone(),
                        created_utc_ms: now_ms()?,
                        command: IntentCommand::OpenApiClose {
                            native_account_id: account,
                            symbol_id,
                            side,
                            quantity: quantity.to_string(),
                            position_id: target,
                            envelope_id: envelope.clone(),
                        },
                    })
                    .await?;
                self.lease
                    .client()
                    .begin_close_position(
                        PositionCloseRequest {
                            expected_account_id: account,
                            expected_revision: revision,
                            position_id: target,
                            volume,
                            client_message_id: envelope.clone(),
                        },
                        self.lease.admission_guard(),
                    )
                    .await
            } else {
                _request_lifetime = requests
                    .begin(OrderIntent {
                        client_order_id: cid.clone(),
                        created_utc_ms: now_ms()?,
                        command: IntentCommand::OpenApiMarket {
                            native_account_id: account,
                            symbol_id,
                            side,
                            quantity: quantity.to_string(),
                            position_id: position,
                            envelope_id: envelope.clone(),
                        },
                    })
                    .await?;
                self.lease
                    .client()
                    .begin_market_order(
                        MarketOrderRequest {
                            expected_account_id: account,
                            expected_revision: revision,
                            symbol_id,
                            side,
                            volume,
                            position_id: position,
                            client_order_id: cid.clone(),
                            client_message_id: envelope.clone(),
                        },
                        self.lease.admission_guard(),
                    )
                    .await
            }
        } else if kind == "cancel" {
            let order_id = number(&command, "venue_order_id")?;
            _request_lifetime = requests
                .begin(OrderIntent {
                    client_order_id: cid.clone(),
                    created_utc_ms: now_ms()?,
                    command: IntentCommand::OpenApiCancel {
                        native_account_id: account,
                        venue_order_id: order_id,
                        original_client_order_id: command["original_client_order_id"]
                            .as_str()
                            .map(str::to_owned),
                        envelope_id: envelope.clone(),
                    },
                })
                .await?;
            self.lease
                .client()
                .begin_cancel_order(
                    CancelOrderRequest {
                        expected_account_id: account,
                        expected_revision: revision,
                        order_id,
                        client_message_id: envelope.clone(),
                    },
                    self.lease.admission_guard(),
                )
                .await
        } else {
            bail!("Unsupported mutation kind")
        };
        let ticket = match intent {
            Ok(ticket) => ticket,
            Err(e) => {
                if !e.may_have_been_sent {
                    requests
                        .resolve(
                            &cid,
                            IntentResolution {
                                kind: ResolutionKind::NotSent,
                                evidence: json!({"reason":e.reason}),
                            },
                        )
                        .await?;
                }
                if e.may_have_been_sent {}
                return Ok(
                    json!({"state":if e.may_have_been_sent{"unknown"}else{"not_sent"},"reason":e.reason,"may_have_been_sent":e.may_have_been_sent}),
                );
            }
        };
        requests
            .written_openapi(&cid, ticket.client_message_id()?)
            .await?;
        let response = match ticket.response().await {
            Ok(v) => v,
            Err(e) => {
                return Ok(
                    json!({"state":"unknown","reason":format!("{e:#}"),"may_have_been_sent":true}),
                );
            }
        };
        let (state, response) = match response {
            OrderMutationReply::Execution(r) => ("execution", r),
            OrderMutationReply::RequestRejected(r) => ("unknown", r),
        };
        if state == "unknown" {
            requests.observe(&cid,json!({"payload_type":response.payload_type,"raw":response.payload,
                "client_message_id":response.client_message_id,"received_mono_ns":response.receive_mono_ns})).await?;
        }
        if kind == "close" && state == "execution" {
            requests.observe(&cid,json!({"payload_type":response.payload_type,"raw":response.payload,
                "client_message_id":response.client_message_id,"received_mono_ns":response.receive_mono_ns,
                "dispatch_start_mono_ns":response.dispatch_start_mono_ns,"dispatch_completed_mono_ns":response.dispatch_completed_mono_ns})).await?;
            self.persist_order(response.payload.clone(), Some(&cid))
                .await?;
        } else {
            self.persist_order(response.payload.clone(), Some(&cid))
                .await?;
        }
        Ok(
            json!({"state":state,"may_have_been_sent":true,"payload_type":response.payload_type,"payload":response.payload,
            "received_mono_ns":response.receive_mono_ns,"dispatch_start_mono_ns":response.dispatch_start_mono_ns,
            "dispatch_completed_mono_ns":response.dispatch_completed_mono_ns,"epoch_healthy":response.epoch_healthy,"local_client_order_id":cid}),
        )
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(task) = self.metadata_task.take() {
            task.abort();
        }
    }
}
impl Connection {
    pub fn acknowledge_executions(&self, through: u64) -> Result<()> {
        self.originals
            .lock()
            .map_err(|_| anyhow::anyhow!("Original execution state poisoned"))?
            .acknowledge(through);
        Ok(())
    }
}

impl Connection {
    pub async fn observe(&self) -> Result<Value> {
        let api = self.lease.client();
        ensure!(api.is_connected().await, "Observer account disconnected");
        let started_mono_ns = monotonic_ns()?;
        let before = api.account_revision();
        let trader = self.request(ReadOnlyRequest::Trader, json!({})).await?;
        let t = BrokerTrader::from_wire(trader.get("trader").context("Missing observer Trader")?)?;
        let reconcile = self.request(ReadOnlyRequest::Reconcile, json!({})).await?;
        let assets = self.request(ReadOnlyRequest::Assets, json!({})).await?;
        let asset = rows(&assets, "asset")?
            .iter()
            .filter(|a| number(a, "assetId").ok() == Some(t.account.deposit_asset_id))
            .collect::<Vec<_>>();
        ensure!(asset.len() == 1, "Missing or ambiguous deposit asset");
        let currency = asset[0]["name"]
            .as_str()
            .context("Missing deposit currency")?;
        let mut positions = Vec::new();
        let mut orders = Vec::new();
        for row in rows(&reconcile, "position")? {
            positions.push(serde_json::to_value(BrokerPosition::from_wire(row)?)?);
        }
        for row in rows(&reconcile, "order")? {
            orders.push(serde_json::to_value(BrokerOrder::from_wire(row)?)?);
        }
        ensure!(
            positions.len() <= self.bound && orders.len() <= self.bound,
            "Observer inventory exceeds bound"
        );
        let after = api.account_revision();
        let completed_mono_ns = monotonic_ns()?;
        ensure!(
            api.is_connected().await,
            "Observer epoch ended during observation"
        );
        Ok(
            json!({"schema_version":1,"account":{"account_id":format!("CTRADER-{}",t.account.trader_login),
            "trader_login":t.account.trader_login.to_string(),"ctid_trader_account_id":t.account.ctid_trader_account_id.to_string(),
            "deposit_asset_id":t.account.deposit_asset_id.to_string(),"environment":if self.live{"live"}else{"demo"},"currency":currency},
            "complete":{"positions":before==after,"orders":before==after},"positions":positions,"orders":orders,
            "observation":{"epoch":self.lease.epoch(),"revision_before":before,"revision_after":after,
                "started_mono_ns":started_mono_ns,"completed_mono_ns":completed_mono_ns},
            "raw":{"trader":trader,"reconcile":reconcile,"assets":assets}}),
        )
    }
    pub async fn check_epoch(&self, epoch: u64, revision: Option<u64>) -> Result<()> {
        ensure!(self.lease.epoch() == epoch, "Account epoch mismatch");
        self.lease.admission_guard().check()?;
        ensure!(
            self.lease.client().is_connected().await,
            "Account epoch ended"
        );
        ensure!(
            revision.is_none_or(|r| r == self.lease.client().account_revision()),
            "Account revision changed"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;
