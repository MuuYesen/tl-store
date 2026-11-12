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

//! Bounded same-lease original account evidence for AccountState.
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct RawSpots {
    epoch: u64,
    account: u64,
    required: HashSet<u64>,
    listened: HashSet<u64>,
    sides: HashMap<(u64, &'static str), (i64, u64, Arc<Value>)>,
}
fn integer(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}
impl RawSpots {
    pub(crate) fn new(epoch: u64, account: u64) -> Option<Self> {
        (epoch > 0 && account > 0).then_some(Self {
            epoch,
            account,
            ..Self::default()
        })
    }
    pub(crate) fn set_required(&mut self, symbols: HashSet<u64>) -> bool {
        if self.listened.union(&symbols).count() > MAX_SYMBOLS || symbols.contains(&0) {
            return false;
        }
        // The shared broker subscription stays live while the account is flat.
        // Retain its latest originals, but export only this observation's scope.
        self.listened.extend(symbols.iter().copied());
        self.required = symbols;
        true
    }
    pub(crate) fn record(&mut self, epoch: u64, payload: &Value, received_mono_ns: u64) {
        if epoch != self.epoch
            || received_mono_ns == 0
            || integer(&payload["ctidTraderAccountId"]) != Some(self.account)
        {
            return;
        }
        let Some(symbol) = integer(&payload["symbolId"]) else {
            return;
        };
        if !self.listened.contains(&symbol) {
            return;
        }
        let Some(timestamp) = payload
            .get("timestamp")
            .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
            .filter(|v| *v > 0)
        else {
            return;
        };
        let payload = Arc::new(payload.clone());
        for side in ["bid", "ask"] {
            let Some(price) = payload.get(side).and_then(integer).filter(|v| *v > 0) else {
                continue;
            };
            let _ = price;
            let key = (symbol, side);
            if self.sides.get(&key).is_none_or(|(old_ts, old_receive, _)| {
                (timestamp, received_mono_ns) > (*old_ts, *old_receive)
            }) {
                self.sides
                    .insert(key, (timestamp, received_mono_ns, Arc::clone(&payload)));
            }
        }
    }
    fn originals(&self, lost_before: u64) -> Vec<SpotRef<'_>> {
        let count = self.required.len().saturating_mul(2);
        let mut seen = HashSet::with_capacity(count);
        let mut out = Vec::with_capacity(count);
        for symbol in &self.required {
            for side in ["bid", "ask"] {
                if let Some((_, received, payload)) = self.sides.get(&(*symbol, side)) {
                    if *received > lost_before && seen.insert(Arc::as_ptr(payload)) {
                        out.push(SpotRef { payload: payload.as_ref(), received_mono_ns: *received });
                    }
                }
            }
        }
        out
    }
    fn fresh(&self, now_mono: u64, now_utc: u64, lost_before: u64) -> bool {
        self.required.iter().all(|symbol| ["bid", "ask"].iter().all(|side| {
            self.sides.get(&(*symbol, *side)).is_some_and(|(source, received, _)| {
                *received > lost_before
                    && now_mono.checked_sub(*received).is_some_and(|age| age <= 1_500_000_000)
                    && u64::try_from(*source).ok()
                        .and_then(|stamp| (now_utc / 1_000_000).checked_sub(stamp))
                        .is_some_and(|age| age <= 1500)
            })
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn large_shared_spot_set_is_deduplicated_by_original_event_identity() {
        let mut cache = RawSpots::new(1, 42).unwrap();
        assert!(cache.set_required((1..=MAX_SYMBOLS as u64).collect()));
        for symbol in 1..=MAX_SYMBOLS as u64 {
            cache.record(1,&json!({"ctidTraderAccountId":"42","symbolId":symbol,"bid":"100","ask":"101","timestamp":"10","extra":"01234567890123456789012345678901"}),symbol);
            let bid = &cache.sides[&(symbol, "bid")].2;
            let ask = &cache.sides[&(symbol, "ask")].2;
            assert!(Arc::ptr_eq(bid, ask));
        }
        let rows = cache.originals(0);
        assert_eq!(rows.len(), MAX_SYMBOLS);
        assert!(encoded_bytes(&rows, MAX_BYTES).is_err());
        assert!(encoded_bytes(&json!({"metadata":"x".repeat(MAX_BYTES)}), MAX_BYTES).is_err());
    }
    #[test]
    fn oversized_metadata_and_borrowed_frame_fail_before_owned_frame_construction() {
        let oversized = json!({"metadata":"x".repeat(MAX_BYTES)});
        let mut evidence = Vec::new();
        let mut used = 2;
        assert!(
            push_metadata(
                &mut evidence,
                &mut used,
                "Symbols",
                json!({}),
                oversized.clone()
            )
            .is_err()
        );
        assert!(evidence.is_empty());
        let account = json!({});
        let capture = json!({});
        let frame = RawFrame {
            schema_version: "leafquant.ctrader-raw-facts.v2",
            kind: "snapshot",
            account: &account,
            responses: vec![ResponseRef::Existing(&oversized)],
            spots: vec![],
            metadata_observed_mono_ns: 1,
            spot_evidence_status: "incomplete",
            capture: &capture,
        };
        assert!(encoded_bytes(&frame, MAX_BYTES).is_err());
    }
    #[test]
    fn separate_side_events_remain_separate_even_at_the_same_receive_time() {
        let mut cache = RawSpots::new(1, 42).unwrap();
        assert!(cache.set_required(HashSet::from([7])));
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":7,"bid":"100","timestamp":"10"}),
            100,
        );
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":7,"ask":"101","timestamp":"10"}),
            100,
        );
        assert!(!Arc::ptr_eq(
            &cache.sides[&(7, "bid")].2,
            &cache.sides[&(7, "ask")].2
        ));
        assert_eq!(cache.originals(0).len(), 2);
    }
    #[test]
    fn dropped_or_unclocked_event_marks_loss_without_blocking_reader() {
        let carrier = Carrier::new(1, 42);
        spot(
            &carrier.shared,
            &carrier.dropped,
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"100"}),
            100,
        );
        assert_eq!(carrier.dropped.load(Ordering::Acquire), 100);
        let guard = carrier.shared.lock().unwrap();
        spot(
            &carrier.shared,
            &carrier.dropped,
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"100","timestamp":"1"}),
            200,
        );
        assert_eq!(carrier.dropped.load(Ordering::Acquire), 200);
        drop(guard);
    }
    #[test]
    fn each_side_keeps_its_real_event_and_clock() {
        let mut cache = RawSpots::new(1, 42).unwrap();
        assert!(cache.set_required(HashSet::from([7])));
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"100","timestamp":"10"}),
            100,
        );
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","ask":"101","timestamp":"11"}),
            200,
        );
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"99","timestamp":"9"}),
            300,
        );
        cache.record(
            1,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"102"}),
            400,
        );
        cache.record(
            2,
            &json!({"ctidTraderAccountId":"42","symbolId":"7","bid":"103","timestamp":"12"}),
            500,
        );
        let rows: Vec<Value> =
            serde_json::from_value(serde_json::to_value(cache.originals(0)).unwrap()).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .any(|r| r["payload"]["bid"] == "100" && r["received_mono_ns"] == 100)
        );
        assert!(
            rows.iter()
                .any(|r| r["payload"]["ask"] == "101" && r["received_mono_ns"] == 200)
        );
    }
}

use crate::openapi::{OpenApiClient, ReadOnlyRequest};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::io;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::watch;

const MAX_SYMBOLS: usize = 2048;
// Bound per AccountState; the existing one-minute purge bounds normal retained frames.
const MAX_BYTES: usize = 256 * 1024;

// Same bounded counting-writer pattern as execution/raw_observations.rs.
struct ByteLimit {
    bytes: usize,
    limit: usize,
}
impl io::Write for ByteLimit {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        if value.len() > self.limit.saturating_sub(self.bytes) {
            return Err(io::Error::other("Raw account evidence exceeds byte budget"));
        }
        self.bytes += value.len();
        Ok(value.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded_bytes<T: Serialize + ?Sized>(value: &T, limit: usize) -> Result<usize> {
    let mut counter = ByteLimit { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.bytes)
}
#[derive(Serialize)]
struct SpotRef<'a> {
    payload: &'a Value,
    received_mono_ns: u64,
}
#[derive(Serialize)]
#[serde(untagged)]
enum ResponseRef<'a> {
    Existing(&'a Value),
    Core {
        operation: &'static str,
        request: &'a Value,
        response: &'a Value,
    },
}
#[derive(Serialize)]
struct RawFrame<'a> {
    schema_version: &'static str,
    kind: &'static str,
    account: &'a Value,
    responses: Vec<ResponseRef<'a>>,
    spots: Vec<SpotRef<'a>>,
    metadata_observed_mono_ns: u64,
    spot_evidence_status: &'static str,
    capture: &'a Value,
}

#[derive(Default)]
pub(crate) struct Metadata {
    needed: HashSet<u64>,
    responses: Vec<Value>,
    spots: Option<RawSpots>,
    ready: bool,
    metadata_mono_ns: u64,
    changed: Arc<tokio::sync::Notify>,
}
pub(crate) type Shared = Arc<Mutex<Metadata>>;

pub(crate) fn new(epoch: u64, account: u64) -> Shared {
    Arc::new(Mutex::new(Metadata {
        spots: RawSpots::new(epoch, account),
        ..Metadata::default()
    }))
}

fn id(v: &Value, key: &str) -> Result<u64> {
    integer(&v[key])
        .filter(|v| *v > 0)
        .context("Missing broker identity")
}
fn rows<'a>(v: &'a Value, key: &str) -> Result<&'a [Value]> {
    match v.get(key) {
        None => Ok(&[]), // repeated proto field omitted when empty
        Some(v) => v
            .as_array()
            .map(Vec::as_slice)
            .context("Malformed repeated broker field"),
    }
}
pub(crate) fn required(reconcile: &Value) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    for key in ["position", "order"] {
        let rows = rows(reconcile, key)?;
        ensure!(
            rows.len() <= MAX_SYMBOLS,
            "Raw account inventory budget exceeded"
        );
        for row in rows {
            ids.insert(id(&row["tradeData"], "symbolId")?);
        }
    }
    ensure!(
        ids.len() <= MAX_SYMBOLS,
        "Raw account symbol budget exceeded"
    );
    Ok(ids)
}
pub(crate) fn spot(
    shared: &Shared,
    dropped: &AtomicU64,
    epoch: u64,
    payload: &Value,
    received: u64,
) {
    let source_time = payload
        .get("timestamp")
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
        .is_some_and(|v| v > 0);
    let sides_valid = ["bid", "ask"].iter().all(|side| {
        payload
            .get(*side)
            .is_none_or(|v| integer(v).is_some_and(|p| p > 0))
    });
    if !source_time || !sides_valid || encoded_bytes(payload, 4096).is_err() {
        dropped.fetch_max(received, Ordering::AcqRel);
        return;
    }
    if let Ok(mut s) = shared.try_lock() {
        if let Some(spots) = &mut s.spots {
            spots.record(epoch, payload, received);
        }
        s.changed.notify_waiters();
    } else {
        dropped.fetch_max(received, Ordering::AcqRel);
    }
}
fn entry(operation: &str, request: Value, response: Value) -> Value {
    json!({"operation": operation, "request": request, "response": response})
}
fn push_metadata(
    rows: &mut Vec<Value>,
    used: &mut usize,
    operation: &'static str,
    request: Value,
    response: Value,
) -> Result<()> {
    let borrowed = ResponseRef::Core {
        operation,
        request: &request,
        response: &response,
    };
    let bytes = encoded_bytes(&borrowed, MAX_BYTES.saturating_sub(*used))?;
    *used = used
        .checked_add(bytes + 1)
        .context("Raw metadata budget overflow")?;
    ensure!(*used <= MAX_BYTES, "Raw metadata budget exceeded");
    rows.push(entry(operation, request, response));
    Ok(())
}
fn push_unavailable(
    rows: &mut Vec<Value>,
    used: &mut usize,
    operation: &'static str,
    request: Value,
) -> Result<()> {
    let row = json!({"operation":operation,"request":request,"error":"request_unavailable"});
    let bytes = encoded_bytes(&row, MAX_BYTES.saturating_sub(*used))?;
    *used = used
        .checked_add(bytes + 1)
        .context("Raw metadata budget overflow")?;
    ensure!(*used <= MAX_BYTES, "Raw metadata budget exceeded");
    rows.push(row);
    Ok(())
}

/// Metadata is gathered asynchronously from the same client; failures never block funds.
pub(crate) async fn worker(
    api: OpenApiClient,
    shared: Shared,
    mut changes: watch::Receiver<Option<HashSet<u64>>>,
    deposit: u64,
) {
    let mut subscribed = HashSet::new();
    loop {
        if changes.changed().await.is_err() {
            break;
        }
        let Some(needed) = changes.borrow_and_update().clone() else {
            continue;
        };
        if let Ok(mut state) = shared.lock() {
            if state.ready
                && state.needed == needed
                && crate::clock::monotonic_ns()
                    .ok()
                    .and_then(|now| now.checked_sub(state.metadata_mono_ns))
                    .is_some_and(|age| age < 20_000_000_000)
            {
                continue;
            }
            // Refresh before the 30-second capture fence. Keep the immutable
            // previous transcript available while it is still fresh; capture
            // independently checks its exact symbol set, epoch and hard age.
        }
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut evidence = Vec::new();
            let mut evidence_bytes = 2usize;
            let light = api.request(ReadOnlyRequest::Symbols, json!({})).await?;
            encoded_bytes(&light, MAX_BYTES)?;
            let mut quotes = HashSet::new();
            let mut found = HashSet::new();
            for row in rows(&light, "symbol")? {
                let symbol = id(row, "symbolId")?;
                if needed.contains(&symbol) {
                    ensure!(found.insert(symbol), "Duplicate light symbol");
                    quotes.insert(id(row, "quoteAssetId")?);
                }
            }
            ensure!(found == needed, "Missing account symbol metadata");
            push_metadata(
                &mut evidence,
                &mut evidence_bytes,
                "Symbols",
                json!({}),
                light,
            )?;
            let mut ids = needed.iter().copied().collect::<Vec<_>>();
            ids.sort_unstable();
            let mut leverage = HashSet::new();
            for chunk in ids.chunks(50) {
                let req = json!({"symbolId":chunk});
                let detail = api
                    .request(ReadOnlyRequest::SymbolDetails, req.clone())
                    .await?;
                encoded_bytes(&detail, MAX_BYTES)?;
                let mut found = HashSet::new();
                for row in rows(&detail, "symbol")? {
                    let symbol = id(row, "symbolId")?;
                    ensure!(
                        chunk.contains(&symbol) && found.insert(symbol),
                        "Unexpected symbol details"
                    );
                    if row.get("leverageId").is_some() {
                        leverage.insert(id(row, "leverageId")?);
                    }
                }
                ensure!(found.len() == chunk.len(), "Missing symbol details");
                push_metadata(
                    &mut evidence,
                    &mut evidence_bytes,
                    "SymbolDetails",
                    req,
                    detail,
                )?;
            }
            let mut leverages = leverage.into_iter().collect::<Vec<_>>();
            leverages.sort_unstable();
            for leverage in leverages {
                let req = json!({"leverageId":leverage});
                match api
                    .request(ReadOnlyRequest::DynamicLeverage, req.clone())
                    .await
                {
                    Ok(response) => push_metadata(
                        &mut evidence,
                        &mut evidence_bytes,
                        "DynamicLeverage",
                        req,
                        response,
                    )?,
                    Err(_) => push_unavailable(
                        &mut evidence,
                        &mut evidence_bytes,
                        "DynamicLeverage",
                        req,
                    )?,
                }
            }
            let mut required = needed.clone();
            let mut quotes = quotes.into_iter().collect::<Vec<_>>();
            quotes.sort_unstable();
            for quote in quotes.into_iter().filter(|q| *q != deposit) {
                let req = json!({"firstAssetId":quote,"lastAssetId":deposit});
                match api
                    .request(ReadOnlyRequest::SymbolsForConversion, req.clone())
                    .await
                {
                    Ok(response) => {
                        encoded_bytes(&response, MAX_BYTES)?;
                        for row in rows(&response, "symbol")? {
                            required.insert(id(row, "symbolId")?);
                        }
                        push_metadata(
                            &mut evidence,
                            &mut evidence_bytes,
                            "SymbolsForConversion",
                            req,
                            response,
                        )?;
                    }
                    Err(_) => push_unavailable(
                        &mut evidence,
                        &mut evidence_bytes,
                        "SymbolsForConversion",
                        req,
                    )?,
                }
            }
            ensure!(required.len() <= MAX_SYMBOLS, "Raw spot budget exceeded");
            // Never unsubscribe a shared account socket: another lease may need the symbol.
            ensure!(
                subscribed.union(&required).count() <= MAX_SYMBOLS,
                "Lifetime spot budget exceeded"
            );
            let mut add = required
                .difference(&subscribed)
                .copied()
                .collect::<Vec<_>>();
            add.sort_unstable();
            // Mark before subscription so an immediate broker spot is retained.
            if let Ok(mut state) = shared.lock() {
                if let Some(spots) = &mut state.spots {
                    ensure!(spots.set_required(required.clone()), "Raw spot lifetime budget exceeded");
                }
            }
            for chunk in add.chunks(50) {
                api.request(
                    ReadOnlyRequest::SubscribeSpots,
                    json!({"symbolId":chunk,"subscribeToSpotTimestamp":true}),
                )
                .await?;
            }
            subscribed.extend(required);
            encoded_bytes(&evidence, MAX_BYTES)?;
            Ok::<_, anyhow::Error>(evidence)
        })
        .await;
        if let Ok(Ok(evidence)) = result {
            if changes.borrow().as_ref() == Some(&needed) {
                if let Ok(mut state) = shared.lock() {
                    state.needed = needed;
                    state.responses = evidence;
                    state.metadata_mono_ns = crate::clock::monotonic_ns().unwrap_or(0);
                    state.ready = state.metadata_mono_ns > 0;
                    state.changed.notify_waiters();
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}

#[derive(Clone)]
pub(crate) struct Carrier {
    pub shared: Shared,
    pub dropped: Arc<AtomicU64>,
    changes: watch::Sender<Option<HashSet<u64>>>,
    epoch: u64,
    session_id: String,
}
impl Carrier {
    pub(crate) fn new(epoch: u64, account: u64) -> Self {
        let (changes, _) = watch::channel(None);
        Self {
            shared: new(epoch, account),
            dropped: Arc::new(AtomicU64::new(0)),
            changes,
            epoch,
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }
    pub(crate) async fn run(self, api: OpenApiClient, deposit: u64) {
        let mut changes = self.changes.subscribe();
        changes.mark_changed(); // A request may precede the spawned worker's first poll.
        worker(api, self.shared.clone(), changes, deposit).await;
    }
    /// Prepare only for this explicit query, before its funds observation window.
    pub(crate) async fn prepare_metadata(&self, needed: HashSet<u64>) -> Result<()> {
        let changed = self.shared.lock()
            .map_err(|_| anyhow::anyhow!("Raw metadata state poisoned"))?.changed.clone();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut requested = false;
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let ready = {
                let state = self.shared.lock()
                    .map_err(|_| anyhow::anyhow!("Raw metadata state poisoned"))?;
                state.ready && state.needed == needed
                    && crate::clock::monotonic_ns()?.checked_sub(state.metadata_mono_ns)
                        .is_some_and(|age| age < 20_000_000_000)
            };
            if ready { return Ok(()); }
            if !requested {
                self.changes.send_replace(Some(needed.clone()));
                requested = true;
            }
            tokio::time::timeout_at(deadline, notified).await
                .context("Raw account metadata preparation timed out")?;
        }
    }

    /// Only account observation waits for evidence; execution never waits here.
    pub(crate) async fn prepare_evidence(&self, needed: HashSet<u64>) -> Result<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        tokio::time::timeout_at(deadline, self.prepare_metadata(needed.clone())).await
            .context("Raw evidence preparation timed out")??;
        let changed = self.shared.lock()
            .map_err(|_| anyhow::anyhow!("Raw metadata state poisoned"))?.changed.clone();
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let ready = {
                let state = self.shared.lock()
                    .map_err(|_| anyhow::anyhow!("Raw metadata state poisoned"))?;
                state.ready && state.needed == needed && state.spots.as_ref().is_some_and(|spots| {
                    spots.fresh(crate::clock::monotonic_ns().unwrap_or(0),
                        crate::engine::wall_ns().unwrap_or(0), self.dropped.load(Ordering::Acquire))
                })
            };
            if ready { return Ok(()); }
            tokio::time::timeout_at(deadline, notified).await
                .context("Original bid/ask preparation timed out")?;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn capture(
        &self,
        api: &OpenApiClient,
        expected: &crate::openapi::funds::ExpectedFundsAccount,
        currency: &str,
        is_live: bool,
        window: &crate::openapi::funds::FundsWindow,
        started_utc: u64,
        assets: &Value,
        core: [&Value; 5],
    ) -> Value {
        let result = (|| -> Result<Value> {
            let needed = required(core[3])?;
            let lost_before = self.dropped.load(Ordering::Acquire);
            self.changes.send_replace(Some(needed.clone()));
            let state = self
                .shared
                .try_lock()
                .map_err(|_| anyhow::anyhow!("Raw metadata busy"))?;
            ensure!(
                state.ready && state.needed == needed,
                "Raw metadata pending"
            );
            ensure!(
                self.epoch == window.epoch_before
                    && window.revision_before == window.revision_after
                    && api.account_revision() == window.revision_after,
                "Raw account revision changed"
            );
            let after_mono = crate::clock::monotonic_ns()?;
            let after_utc = crate::engine::wall_ns()?;
            ensure!(
                after_mono
                    .checked_sub(window.completed_mono_ns)
                    .is_some_and(|age| age <= 3_000_000_000),
                "Raw observation expired"
            );
            ensure!(
                after_mono
                    .checked_sub(state.metadata_mono_ns)
                    .is_some_and(|age| age <= 30_000_000_000),
                "Raw metadata expired"
            );
            // Quote validity and FX path completeness are decided downstream from original facts.
            // This marker only says whether both original sides have arrived recently.
            let sides_present = state.spots.as_ref().is_some_and(|spots| {
                spots.fresh(after_mono, after_utc, lost_before)
            }) && !state.responses.iter().any(|row| {
                row["operation"] == "SymbolsForConversion" && row.get("error").is_some()
            });
            let point = |mono, utc| json!({"session_id":self.session_id,"epoch":self.epoch,"revision":window.revision_after,"mono_ns":mono,"utc_ns":utc});
            let capture = json!({"before":point(window.started_mono_ns,started_utc),"core":point(window.completed_mono_ns,window.completed_utc_ns),"after":point(after_mono,after_utc)});
            let account = json!({"account_id":format!("CTRADER-{}",expected.trader_login),
                "trader_login":expected.trader_login.to_string(),"ctid_trader_account_id":expected.ctid_trader_account_id.to_string(),
                "deposit_asset_id":expected.deposit_asset_id.to_string(),"currency":currency,
                "environment":if is_live {"live"} else {"demo"}});
            let empty = json!({});
            let mut responses = Vec::with_capacity(state.responses.len() + 6);
            responses.push(ResponseRef::Core {
                operation: "Assets",
                request: &empty,
                response: assets,
            });
            responses.extend(
                state
                    .responses
                    .iter()
                    .filter(|r| r["operation"] == "Symbols")
                    .map(ResponseRef::Existing),
            );
            for (operation, response) in
                ["Trader", "Reconcile", "PositionPnL", "Reconcile", "Trader"]
                    .into_iter()
                    .zip(core)
            {
                responses.push(ResponseRef::Core {
                    operation,
                    request: &empty,
                    response,
                });
            }
            responses.extend(
                state
                    .responses
                    .iter()
                    .filter(|r| r["operation"] != "Symbols")
                    .map(ResponseRef::Existing),
            );
            let frame = RawFrame {
                schema_version: "leafquant.ctrader-raw-facts.v2",
                kind: "snapshot",
                account: &account,
                responses,
                spots: state
                    .spots
                    .as_ref()
                    .map(|spots| spots.originals(lost_before))
                    .unwrap_or_default(),
                metadata_observed_mono_ns: state.metadata_mono_ns,
                spot_evidence_status: if sides_present {
                    "sides_recent_unverified"
                } else {
                    "incomplete"
                },
                capture: &capture,
            };
            // Count borrowed originals before making any AccountState-sized clone.
            encoded_bytes(&frame, MAX_BYTES)?;
            let raw = serde_json::to_value(&frame)?;
            drop(frame);
            drop(state);
            ensure!(
                api.account_revision() == window.revision_after
                    && self.dropped.load(Ordering::Acquire) == lost_before,
                "Raw account changed or spot evidence was lost during capture"
            );
            Ok(raw)
        })();
        result.unwrap_or_else(
            |_| json!({"status":"incomplete","reason":"raw_account_evidence_unavailable"}),
        )
    }
}

#[cfg(test)]
mod websocket_tests {
    use super::*;
    use crate::openapi::{OpenApiConfig, tests::mock_credentials};
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[tokio::test]
    async fn metadata_and_original_spot_sides_share_real_websocket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let request: Value = serde_json::from_str(&text).unwrap();
                let kind = request["payloadType"].as_u64().unwrap();
                if kind == 51 {
                    continue;
                }
                let (kind_out, payload) = match kind {
                    2100 => (2101, json!({})),
                    2149 => (
                        2150,
                        json!({"accessToken":"mock-token","permissionScope":0,"ctidTraderAccount":[{"ctidTraderAccountId":"9","traderLogin":"123","isLive":false}]}),
                    ),
                    2102 => (2103, json!({"ctidTraderAccountId":"9"})),
                    2121 => (
                        2122,
                        json!({"ctidTraderAccountId":"9","trader":{"ctidTraderAccountId":"9","traderLogin":"123","depositAssetId":"2","balance":"10000","moneyDigits":2,"accountType":0,"accessRights":0}}),
                    ),
                    2114 => (
                        2115,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"7","quoteAssetId":"3"}]}),
                    ),
                    2116 => (
                        2117,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"7"}]}),
                    ),
                    2118 => (
                        2119,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"8"}]}),
                    ),
                    2127 => (2128, json!({"ctidTraderAccountId":"9"})),
                    _ => panic!("unexpected mock request {kind}"),
                };
                socket.send(Message::Text(json!({"clientMsgId":request["clientMsgId"],"payloadType":kind_out,"payload":payload}).to_string().into())).await.unwrap();
                if kind == 2127 {
                    for (side, price, time) in [("bid", "100", "100"), ("ask", "101", "101")] {
                        let mut payload =
                            json!({"ctidTraderAccountId":"9","symbolId":"7","timestamp":time});
                        payload[side] = json!(price);
                        socket
                            .send(Message::Text(
                                json!({"payloadType":2131,"payload":payload})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                    }
                }
            }
        });
        let api =
            OpenApiClient::connect_to(&endpoint, OpenApiConfig::demo(123), mock_credentials())
                .await
                .unwrap();
        let mut events = api.take_events().unwrap();
        let carrier = Carrier::new(1, 9);
        let state = carrier.shared.clone();
        let dropped = carrier.dropped.clone();
        let reader = tokio::spawn(async move {
            while let Some(e) = events.next().await {
                if e.payload_type == 2131 {
                    spot(&state, &dropped, 1, &e.payload, e.received_mono_ns);
                }
            }
        });
        carrier.changes.send_replace(Some(HashSet::from([7])));
        let task = tokio::spawn(carrier.clone().run(api.clone(), 2));
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let complete = {
                    let s = carrier.shared.lock().unwrap();
                    s.ready && s.spots.as_ref().unwrap().originals(0).len() == 2
                };
                if complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let state = carrier.shared.lock().unwrap();
        assert!(
            state
                .responses
                .iter()
                .any(|v| v["operation"] == "SymbolsForConversion")
        );
        let spots: Vec<Value> = serde_json::from_value(
            serde_json::to_value(state.spots.as_ref().unwrap().originals(0)).unwrap(),
        )
        .unwrap();
        assert!(spots.iter().any(|v| v["payload"]["bid"] == "100"));
        assert!(spots.iter().any(|v| v["payload"]["ask"] == "101"));
        drop(state);
        let mono = crate::clock::monotonic_ns().unwrap();
        let utc = crate::engine::wall_ns().unwrap();
        let rev = api.account_revision();
        let window = crate::openapi::funds::FundsWindow {
            epoch_before: 1,
            epoch_after: 1,
            revision_before: rev,
            revision_after: rev,
            started_mono_ns: mono,
            completed_mono_ns: mono,
            now_mono_ns: mono,
            max_span_ns: 2_000_000_000,
            max_age_ns: 3_000_000_000,
            completed_utc_ns: utc,
        };
        let expected = crate::openapi::funds::ExpectedFundsAccount {
            ctid_trader_account_id: 9,
            trader_login: 123,
            deposit_asset_id: 2,
        };
        let trader = json!({"ctidTraderAccountId":"9","trader":{}});
        let reconcile = json!({"ctidTraderAccountId":"9","position":[{"positionId":"17","tradeData":{"symbolId":"7"}}],"order":[]});
        let pnl = json!({"ctidTraderAccountId":"9","positionUnrealizedPnL":[]});
        let assets = json!({"ctidTraderAccountId":"9","asset":[]});
        let currency = "USD";
        let raw = carrier.capture(
            &api,
            &expected,
            currency,
            false,
            &window,
            utc,
            &assets,
            [&trader, &reconcile, &pnl, &reconcile, &trader],
        );
        assert_eq!(raw["schema_version"], "leafquant.ctrader-raw-facts.v2");
        assert_eq!(raw["capture"]["before"]["mono_ns"], mono);
        assert_eq!(raw["capture"]["core"]["utc_ns"], utc);
        assert_eq!(raw["spot_evidence_status"], "incomplete"); // real source clock in mock is stale
        let five = raw["responses"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| {
                ["Trader", "Reconcile", "PositionPnL"].contains(&v["operation"].as_str().unwrap())
            })
            .count();
        assert_eq!(five, 5);
        let missing = Carrier::new(2, 9);
        assert_eq!(
            missing.capture(
                &api,
                &expected,
                currency,
                false,
                &window,
                utc,
                &assets,
                [&trader, &reconcile, &pnl, &reconcile, &trader]
            )["status"],
            "incomplete"
        );
        api.close().await;
        reader.abort();
        task.abort();
        server.abort();
    }
    #[tokio::test]
    async fn metadata_prefetch_preserves_valid_capture_and_hard_expiry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let delayed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let server_delayed = delayed.clone(); let server_seen = seen.clone(); let server_resume = resume.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(socket).await.unwrap();
            while let Some(Ok(Message::Text(text))) = socket.next().await {
                let request: Value = serde_json::from_str(&text).unwrap();
                let kind = request["payloadType"].as_u64().unwrap();
                if kind == 51 {
                    continue;
                }
                if kind == 2114 && server_delayed.load(Ordering::Acquire) {
                    server_seen.notify_one(); server_resume.notified().await;
                }
                let (kind_out, payload) = match kind {
                    2100 => (2101, json!({})),
                    2149 => (
                        2150,
                        json!({"accessToken":"mock-token","permissionScope":0,"ctidTraderAccount":[{"ctidTraderAccountId":"9","traderLogin":"123","isLive":false}]}),
                    ),
                    2102 => (2103, json!({"ctidTraderAccountId":"9"})),
                    2121 => (
                        2122,
                        json!({"ctidTraderAccountId":"9","trader":{"ctidTraderAccountId":"9","traderLogin":"123","depositAssetId":"2","balance":"10000","moneyDigits":2,"accountType":0,"accessRights":0}}),
                    ),
                    2114 => (
                        2115,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"7","quoteAssetId":"3"}]}),
                    ),
                    2116 => (
                        2117,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"7"}]}),
                    ),
                    2118 => (
                        2119,
                        json!({"ctidTraderAccountId":"9","symbol":[{"symbolId":"8"}]}),
                    ),
                    2127 => (2128, json!({"ctidTraderAccountId":"9"})),
                    _ => panic!("unexpected mock request {kind}"),
                };
                socket.send(Message::Text(json!({"clientMsgId":request["clientMsgId"],"payloadType":kind_out,"payload":payload}).to_string().into())).await.unwrap();
                if kind == 2127 {
                    for (side, price, time) in [("bid", "100", "100"), ("ask", "101", "101")] {
                        let mut payload =
                            json!({"ctidTraderAccountId":"9","symbolId":"7","timestamp":time});
                        payload[side] = json!(price);
                        socket
                            .send(Message::Text(
                                json!({"payloadType":2131,"payload":payload})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                    }
                }
            }
        });
        let api =
            OpenApiClient::connect_to(&endpoint, OpenApiConfig::demo(123), mock_credentials())
                .await
                .unwrap();
        let mut events = api.take_events().unwrap();
        let carrier = Carrier::new(1, 9);
        let state = carrier.shared.clone();
        let dropped = carrier.dropped.clone();
        let reader = tokio::spawn(async move {
            while let Some(e) = events.next().await {
                if e.payload_type == 2131 {
                    spot(&state, &dropped, 1, &e.payload, e.received_mono_ns);
                }
            }
        });
        let task = tokio::spawn(carrier.clone().run(api.clone(), 2));
        tokio::task::yield_now().await;
        carrier.changes.send_replace(Some(HashSet::from([7])));
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let complete = {
                    let s = carrier.shared.lock().unwrap();
                    s.ready && s.spots.as_ref().unwrap().originals(0).len() == 2
                };
                if complete {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let state = carrier.shared.lock().unwrap();
        assert!(
            state
                .responses
                .iter()
                .any(|v| v["operation"] == "SymbolsForConversion")
        );
        let spots: Vec<Value> = serde_json::from_value(
            serde_json::to_value(state.spots.as_ref().unwrap().originals(0)).unwrap(),
        )
        .unwrap();
        assert!(spots.iter().any(|v| v["payload"]["bid"] == "100"));
        assert!(spots.iter().any(|v| v["payload"]["ask"] == "101"));
        drop(state);
        let mono = crate::clock::monotonic_ns().unwrap();
        let utc = crate::engine::wall_ns().unwrap();
        let rev = api.account_revision();
        let window = crate::openapi::funds::FundsWindow {
            epoch_before: 1,
            epoch_after: 1,
            revision_before: rev,
            revision_after: rev,
            started_mono_ns: mono,
            completed_mono_ns: mono,
            now_mono_ns: mono,
            max_span_ns: 2_000_000_000,
            max_age_ns: 3_000_000_000,
            completed_utc_ns: utc,
        };
        let expected = crate::openapi::funds::ExpectedFundsAccount {
            ctid_trader_account_id: 9,
            trader_login: 123,
            deposit_asset_id: 2,
        };
        let trader = json!({"ctidTraderAccountId":"9","trader":{}});
        let reconcile = json!({"ctidTraderAccountId":"9","position":[{"positionId":"17","tradeData":{"symbolId":"7"}}],"order":[]});
        let pnl = json!({"ctidTraderAccountId":"9","positionUnrealizedPnL":[]});
        let assets = json!({"ctidTraderAccountId":"9","asset":[]});
        let currency = "USD";
        let raw = carrier.capture(
            &api,
            &expected,
            currency,
            false,
            &window,
            utc,
            &assets,
            [&trader, &reconcile, &pnl, &reconcile, &trader],
        );
        assert_eq!(raw["schema_version"], "leafquant.ctrader-raw-facts.v2");
        assert_eq!(raw["capture"]["before"]["mono_ns"], mono);
        assert_eq!(raw["capture"]["core"]["utc_ns"], utc);
        assert_eq!(raw["spot_evidence_status"], "incomplete"); // real source clock in mock is stale
        let five = raw["responses"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| {
                ["Trader", "Reconcile", "PositionPnL"].contains(&v["operation"].as_str().unwrap())
            })
            .count();
        assert_eq!(five, 5);
        let missing = Carrier::new(2, 9);
        assert_eq!(
            missing.capture(
                &api,
                &expected,
                currency,
                false,
                &window,
                utc,
                &assets,
                [&trader, &reconcile, &pnl, &reconcile, &trader]
            )["status"],
            "incomplete"
        );
        let old_stamp = crate::clock::monotonic_ns().unwrap() - 25_000_000_000;
        carrier.shared.lock().unwrap().metadata_mono_ns = old_stamp;
        delayed.store(true, Ordering::Release);
        carrier.changes.send_replace(Some(HashSet::from([7])));
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), seen.notified()).await.is_ok(),
                "Metadata refresh must start before the 30-second hard expiry");
        assert!(carrier.shared.lock().unwrap().ready, "In-flight refresh must not invalidate an unexpired original transcript");
        let preserved = carrier.capture(&api, &expected, currency, false, &window, utc, &assets,
                                       [&trader, &reconcile, &pnl, &reconcile, &trader]);
        assert_eq!(preserved["schema_version"], "leafquant.ctrader-raw-facts.v2");
        assert_eq!(preserved["responses"], raw["responses"], "Refresh must preserve the exact original facts, not synthesize new responses");
        carrier.shared.lock().unwrap().metadata_mono_ns = crate::clock::monotonic_ns().unwrap() - 31_000_000_000;
        let expired = carrier.capture(&api, &expected, currency, false, &window, utc, &assets,
                                     [&trader, &reconcile, &pnl, &reconcile, &trader]);
        assert_eq!(expired["status"], "incomplete", "The hard metadata lifetime must remain enforced");
        resume.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if carrier.shared.lock().unwrap().metadata_mono_ns > old_stamp { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let renewed = carrier.capture(&api, &expected, currency, false, &window, utc, &assets,
                                     [&trader, &reconcile, &pnl, &reconcile, &trader]);
        assert_eq!(renewed["schema_version"], "leafquant.ctrader-raw-facts.v2");
        api.close().await;
        reader.abort();
        task.abort();
        server.abort();
    }
}

#[cfg(test)]
mod request_prepare_tests {
    use super::*;
    include!("metadata_prepare_tests.rs");
}
