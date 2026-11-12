//! Connection-local request correlation. No files, durable replay, or transport-wide lock.
use crate::openapi::{OpenApiEvent, model::BrokerOrder};
use anyhow::{Context, Result, ensure};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Mutex,
};

const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE: usize = 32 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntentCommand {
    OpenApiMarket {
        native_account_id: u64,
        symbol_id: u64,
        side: u32,
        quantity: String,
        position_id: Option<u64>,
        envelope_id: String,
    },
    OpenApiClose {
        native_account_id: u64,
        symbol_id: u64,
        side: u32,
        quantity: String,
        position_id: u64,
        envelope_id: String,
    },
    OpenApiCancel {
        native_account_id: u64,
        venue_order_id: u64,
        original_client_order_id: Option<String>,
        envelope_id: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderIntent {
    pub client_order_id: String,
    pub created_utc_ms: i64,
    pub command: IntentCommand,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntentWriteReceipt {
    OpenApi { client_message_id: String },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingIntent {
    pub intent: OrderIntent,
    pub venue_order_id: Option<u64>,
    pub write_receipt: Option<IntentWriteReceipt>,
}
#[derive(Clone, Debug, PartialEq)]
pub enum ResolutionKind {
    NotSent,
    BrokerTerminal,
}
pub struct IntentResolution {
    pub kind: ResolutionKind,
    pub evidence: Value,
}
struct Entry {
    row: PendingIntent,
    response: Option<Value>,
    complete: bool,
    leased: bool,
    bytes: usize,
}
#[derive(Default)]
struct State {
    rows: BTreeMap<String, Entry>,
    completed: VecDeque<String>,
    bytes: usize,
}
pub struct InFlightRequests {
    account: u64,
    capacity: usize,
    state: Mutex<State>,
}
/// Retains correlation while the owned submit future is alive; does not hold a lock.
pub struct RequestLifetime<'a> {
    owner: &'a InFlightRequests,
    cid: String,
}
impl Drop for RequestLifetime<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            if let Some(entry) = state.rows.get_mut(&self.cid) {
                entry.leased = false;
            }
        }
    }
}
fn integer(v: &Value, k: &str) -> Result<u64> {
    v[k].as_u64()
        .or_else(|| v[k].as_str()?.parse().ok())
        .context(format!("Missing integer {k}"))
}
fn identity(v: &str) -> Result<()> {
    ensure!(
        !v.is_empty()
            && v.len() <= 50
            && v.bytes().all(|b| (0x21..=0x7e).contains(&b) && b != b'|'),
        "Invalid request identity"
    );
    Ok(())
}
fn native(c: &IntentCommand) -> u64 {
    match c {
        IntentCommand::OpenApiMarket {
            native_account_id, ..
        }
        | IntentCommand::OpenApiClose {
            native_account_id, ..
        }
        | IntentCommand::OpenApiCancel {
            native_account_id, ..
        } => *native_account_id,
    }
}
fn envelope(c: &IntentCommand) -> &str {
    match c {
        IntentCommand::OpenApiMarket { envelope_id, .. }
        | IntentCommand::OpenApiClose { envelope_id, .. }
        | IntentCommand::OpenApiCancel { envelope_id, .. } => envelope_id,
    }
}
fn validate_order(row: &PendingIntent, account: u64, raw: &Value) -> Result<BrokerOrder> {
    ensure!(
        integer(raw, "ctidTraderAccountId")? == account,
        "Execution account mismatch"
    );
    let order = BrokerOrder::from_wire(raw.get("order").context("Missing native order")?)?;
    ensure!(
        row.venue_order_id.is_none_or(|id| id == order.order_id),
        "Native order identity changed"
    );
    match &row.intent.command {
        IntentCommand::OpenApiMarket {
            symbol_id,
            side,
            quantity,
            position_id,
            ..
        } => {
            ensure!(
                order.symbol_id == *symbol_id
                    && order.trade_side == *side
                    && order.quantity == quantity.parse::<Decimal>()?
                    && order.client_order_id.as_deref()
                        == Some(row.intent.client_order_id.as_str())
                    && position_id
                        .is_none_or(|id| order.position_id.is_none_or(|actual| actual == id)),
                "Market response differs from request"
            );
        }
        IntentCommand::OpenApiClose {
            symbol_id,
            side,
            quantity,
            position_id,
            ..
        } => {
            ensure!(
                order.symbol_id == *symbol_id
                    && order.trade_side == *side
                    && order.quantity == quantity.parse::<Decimal>()?
                    && order.position_id.is_none_or(|id| id == *position_id)
                    && order.closing_order.is_none_or(|v| v),
                "Close response differs from exact request"
            );
        }
        IntentCommand::OpenApiCancel {
            venue_order_id,
            original_client_order_id,
            ..
        } => {
            ensure!(
                order.order_id == *venue_order_id
                    && original_client_order_id
                        .as_ref()
                        .is_none_or(|cid| order.client_order_id.as_ref() == Some(cid)),
                "Cancel response differs from target"
            );
        }
    }
    Ok(order)
}
impl State {
    fn trim(&mut self, capacity: usize, extra: usize) -> Result<()> {
        while self.rows.len() >= capacity || self.bytes.saturating_add(extra) > MAX_BYTES {
            let count = self.completed.len();
            let mut retired = false;
            for _ in 0..count {
                let cid = self.completed.pop_front().unwrap();
                if self.rows.get(&cid).is_some_and(|e| e.leased) {
                    self.completed.push_back(cid);
                    continue;
                }
                if let Some(old) = self.rows.remove(&cid) {
                    self.bytes = self.bytes.saturating_sub(old.bytes);
                    retired = true;
                    break;
                }
            }
            ensure!(retired, "In-flight request capacity exhausted");
        }
        Ok(())
    }
}
impl InFlightRequests {
    pub fn new(account: u64, capacity: usize) -> Result<Self> {
        ensure!(
            account > 0 && capacity > 0,
            "Explicit bounded account required"
        );
        Ok(Self {
            account,
            capacity: capacity.min(1024),
            state: Mutex::new(State::default()),
        })
    }
    pub async fn begin(&self, intent: OrderIntent) -> Result<RequestLifetime<'_>> {
        identity(&intent.client_order_id)?;
        identity(envelope(&intent.command))?;
        ensure!(
            native(&intent.command) == self.account && intent.created_utc_ms >= 0,
            "Request account or clock mismatch"
        );
        match &intent.command {
            IntentCommand::OpenApiMarket {
                symbol_id,
                side,
                quantity,
                ..
            }
            | IntentCommand::OpenApiClose {
                symbol_id,
                side,
                quantity,
                ..
            } => {
                ensure!(
                    *symbol_id > 0
                        && matches!(side, 1 | 2)
                        && quantity.parse::<Decimal>()? > Decimal::ZERO,
                    "Invalid request terms"
                );
            }
            IntentCommand::OpenApiCancel { venue_order_id, .. } => {
                ensure!(*venue_order_id > 0, "Invalid cancel target")
            }
        }
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        ensure!(
            !s.rows.contains_key(&intent.client_order_id)
                && !s
                    .rows
                    .values()
                    .any(|r| envelope(&r.row.intent.command) == envelope(&intent.command)),
            "Duplicate in-flight request; never resend"
        );
        s.trim(self.capacity, 0)?;
        let venue_order_id = match intent.command {
            IntentCommand::OpenApiCancel { venue_order_id, .. } => Some(venue_order_id),
            _ => None,
        };
        let cid = intent.client_order_id.clone();
        s.rows.insert(
            cid.clone(),
            Entry {
                row: PendingIntent {
                    intent,
                    venue_order_id,
                    write_receipt: None,
                },
                response: None,
                complete: false,
                leased: true,
                bytes: 0,
            },
        );
        Ok(RequestLifetime { owner: self, cid })
    }
    pub async fn pending(&self) -> Result<Vec<PendingIntent>> {
        let s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        Ok(s.rows
            .values()
            .filter(|e| !e.complete)
            .map(|e| e.row.clone())
            .collect())
    }
    pub async fn get(&self, cid: &str) -> Result<Option<PendingIntent>> {
        let s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        Ok(s.rows
            .get(cid)
            .filter(|e| !e.complete)
            .map(|e| e.row.clone()))
    }
    pub async fn written_openapi(&self, cid: &str, mid: &str) -> Result<()> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        let e = s.rows.get_mut(cid).context("Missing in-flight request")?;
        ensure!(
            envelope(&e.row.intent.command) == mid,
            "Flushed envelope mismatch"
        );
        e.row.write_receipt = Some(IntentWriteReceipt::OpenApi {
            client_message_id: mid.into(),
        });
        Ok(())
    }
    pub async fn observe(&self, cid: &str, evidence: Value) -> Result<()> {
        let bytes = serde_json::to_vec(&evidence)?.len();
        ensure!(
            bytes <= MAX_RESPONSE,
            "Native response exceeds observation budget"
        );
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        let old = s.rows.get(cid).context("Missing in-flight request")?;
        ensure!(
            evidence["client_message_id"].as_str() == Some(envelope(&old.row.intent.command)),
            "Response envelope mismatch"
        );
        if evidence["payload_type"] == 2126 && evidence["raw"].get("order").is_some() {
            validate_order(&old.row, self.account, &evidence["raw"])?;
        }
        if let Some(previous) = &old.response {
            ensure!(
                previous["payload_type"] == evidence["payload_type"]
                    && previous["raw"] == evidence["raw"],
                "Conflicting matched response"
            );
            let mut merged = previous.clone();
            for (k, v) in evidence.as_object().context("Response object required")? {
                merged[k] = v.clone();
            }
            s.rows.get_mut(cid).unwrap().response = Some(merged);
        } else {
            ensure!(
                s.bytes.saturating_add(bytes) <= MAX_BYTES,
                "In-flight response budget exhausted"
            );
            let e = s.rows.get_mut(cid).unwrap();
            e.response = Some(evidence);
            e.bytes = bytes;
            s.bytes += bytes;
        }
        Ok(())
    }
    pub async fn bind(&self, cid: &str, venue: u64) -> Result<()> {
        ensure!(venue > 0, "Invalid venue identity");
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        let e = s.rows.get(cid).context("Missing in-flight request")?;
        ensure!(
            e.row.venue_order_id.is_none_or(|id| id == venue),
            "Venue identity changed"
        );
        if !matches!(e.row.intent.command, IntentCommand::OpenApiCancel { .. }) {
            ensure!(
                !s.rows.iter().any(|(other, e)| other != cid
                    && !matches!(e.row.intent.command, IntentCommand::OpenApiCancel { .. })
                    && e.row.venue_order_id == Some(venue)),
                "Venue order bound to another request"
            );
        }
        s.rows.get_mut(cid).unwrap().row.venue_order_id = Some(venue);
        Ok(())
    }
    pub async fn resolve(&self, cid: &str, resolution: IntentResolution) -> Result<()> {
        let mut s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        let e = s.rows.get_mut(cid).context("Missing in-flight request")?;
        match resolution.kind {
            ResolutionKind::NotSent => ensure!(
                e.row.write_receipt.is_none()
                    && (matches!(e.row.intent.command, IntentCommand::OpenApiCancel { .. })
                        || e.row.venue_order_id.is_none()),
                "Written request is not not-sent"
            ),
            ResolutionKind::BrokerTerminal => {
                let order = validate_order(&e.row, self.account, &resolution.evidence["raw"])?;
                ensure!(
                    e.row.venue_order_id == Some(order.order_id)
                        && matches!(order.order_status, 2..=5),
                    "Actual terminal order required"
                );
                ensure!(
                    integer(&resolution.evidence, "order_id")? == order.order_id
                        && integer(&resolution.evidence, "order_status")?
                            == u64::from(order.order_status)
                        && resolution.evidence["quantity"]
                            .as_str()
                            .context("Quantity required")?
                            .parse::<Decimal>()?
                            == order.quantity
                        && resolution.evidence["filled_quantity"]
                            .as_str()
                            .context("Filled quantity required")?
                            .parse::<Decimal>()?
                            == order.filled_quantity,
                    "Terminal summary differs from raw order"
                );
            }
        }
        if !e.complete {
            e.complete = true;
            s.completed.push_back(cid.to_owned());
        }
        Ok(())
    }
    pub async fn correlate_event(&self, event: &OpenApiEvent) -> Result<Option<String>> {
        let Some(mid) = event.client_message_id.as_deref() else {
            return Ok(None);
        };
        let cid = {
            let s = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
            s.rows
                .iter()
                .find(|(_, e)| envelope(&e.row.intent.command) == mid)
                .map(|(cid, _)| cid.clone())
        };
        let Some(cid) = cid else { return Ok(None) };
        if event.payload_type != 2126 {
            return Ok(None);
        }
        // The reader delivered an authenticated exact-envelope response; it proves a real send.
        self.written_openapi(&cid, mid).await?;
        let evidence = json!({"payload_type":event.payload_type,"raw":event.payload,"client_message_id":mid,"received_mono_ns":event.received_mono_ns});
        let already = {
            let s = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
            if event.payload.get("order").is_some() {
                validate_order(&s.rows[&cid].row, self.account, &event.payload)?;
            }
            s.rows[&cid].response.is_some()
        };
        if !already {
            self.observe(&cid, evidence).await?;
        }
        if let Some(order) = event.payload.get("order") {
            let id = integer(order, "orderId")?;
            self.bind(&cid, id).await?;
        }
        Ok(Some(cid))
    }
    pub async fn native_close_binding(&self, cid: &str) -> Result<Option<Value>> {
        let s = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("Request state poisoned"))?;
        let Some(e) = s.rows.get(cid) else {
            return Ok(None);
        };
        let IntentCommand::OpenApiClose {
            native_account_id,
            quantity,
            position_id,
            envelope_id,
            ..
        } = &e.row.intent.command
        else {
            return Ok(None);
        };
        let Some(order_id) = e.row.venue_order_id else {
            return Ok(None);
        };
        let Some(matched) = &e.response else {
            return Ok(None);
        };
        if matched["payload_type"] != 2126 || matched["raw"].get("order").is_none() {
            return Ok(None);
        }
        let order = validate_order(&e.row, self.account, &matched["raw"])?;
        ensure!(
            order.order_id == order_id
                && matched["client_message_id"].as_str() == Some(envelope_id),
            "Matched close response differs from request"
        );
        let wire = quantity
            .parse::<Decimal>()?
            .checked_mul(Decimal::from(100))
            .context("Close volume overflow")?;
        ensure!(
            wire > Decimal::ZERO && wire.fract().is_zero(),
            "Exact close units required"
        );
        let volume = wire.to_i64().context("Close volume overflow")?;
        let request = json!({"clientMsgId":envelope_id,"payloadType":2111,"payload":{"ctidTraderAccountId":native_account_id,"positionId":position_id,"volume":volume}});
        Ok(Some(
            json!({"schema_version":1,"operation":"close_position","pending":e.row,"request":request,"matched_response":matched}),
        ))
    }
}

#[cfg(test)]
mod tests {
    include!("inflight/tests.rs");
}
