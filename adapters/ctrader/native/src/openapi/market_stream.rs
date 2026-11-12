// SPDX-License-Identifier: LGPL-3.0-only
//! Bounded read-only spot and depth observations on an isolated account socket.

use std::{collections::HashMap, sync::Arc, time::UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};

use super::{
    OpenApiAccountSession, OpenApiEvents, OpenApiLease, ReadOnlyRequest,
    depth::DepthBook,
    depth_wire::{apply_event, positive, required},
};

/// One actual spot update, in protocol price units, with independent sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotObservation {
    pub account_id: u64,
    pub symbol_id: u64,
    pub bid: Option<u64>,
    pub ask: Option<u64>,
    pub broker_timestamp_ms: Option<u64>,
    pub received_utc_ns: u64,
    pub received_mono_ns: u64,
}

pub(crate) fn decode_spot(
    account_id: u64,
    symbol_id: u64,
    payload: &Value,
    received_utc_ns: u64,
    received_mono_ns: u64,
) -> Result<Option<SpotObservation>> {
    ensure!(
        required(payload, "ctidTraderAccountId").map_err(|e| anyhow!(e))? == account_id
            && required(payload, "symbolId").map_err(|e| anyhow!(e))? == symbol_id,
        "Spot account or symbol mismatch"
    );
    let bid = payload
        .get("bid")
        .map(positive)
        .transpose()
        .map_err(|e| anyhow!(e))?;
    let ask = payload
        .get("ask")
        .map(positive)
        .transpose()
        .map_err(|e| anyhow!(e))?;
    let broker_timestamp_ms = payload
        .get("timestamp")
        .map(positive)
        .transpose()
        .map_err(|e| anyhow!(e))?;
    if bid.is_none() && ask.is_none() {
        return Ok(None);
    }
    Ok(Some(SpotObservation {
        account_id,
        symbol_id,
        bid,
        ask,
        broker_timestamp_ms,
        received_utc_ns,
        received_mono_ns,
    }))
}

/// Observed levels in protocol units, without a claim of complete coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthObservation {
    pub account_id: u64,
    pub symbol_id: u64,
    /// Prices are 1/100000 and sizes are 1/100 of a unit.
    pub bids: Vec<(u64, u64)>,
    /// Prices are 1/100000 and sizes are 1/100 of a unit.
    pub asks: Vec<(u64, u64)>,
    pub received_utc_ns: u64,
    pub received_mono_ns: u64,
    /// The broker depth event has no source timestamp.
    pub source_ts_ns: Option<u64>,
    /// An incremental event alone does not establish full-book coverage.
    pub complete: bool,
}

/// The distinct raw market observation types carried by the isolated socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketObservation {
    Spot(SpotObservation),
    Depth(DepthObservation),
}

/// A normalized observation and the parsed broker payload, not original wire bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketEvent {
    pub observation: MarketObservation,
    pub raw_payload: Value,
}

/// Broker market payload and receive clocks, without a reconstructed book.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMarketEvent {
    pub payload_type: u32,
    pub account_id: u64,
    pub symbol_id: u64,
    pub received_utc_ns: u64,
    pub received_mono_ns: u64,
    pub raw_payload: Value,
}

impl RawMarketEvent {
    /// JSON encoding retains the parsed broker payload and exact identity strings.
    #[must_use]
    pub fn to_wire(&self) -> Value {
        json!({"schema_version":"leafquant.ctrader-market-raw.v1",
            "source":"ctrader-openapi", "payload_type":self.payload_type,
            "account_id":self.account_id.to_string(), "symbol_id":self.symbol_id.to_string(),
            "received_utc_ns":self.received_utc_ns.to_string(),
            "received_mono_ns":self.received_mono_ns.to_string(),
            "parsed_payload":self.raw_payload})
    }
}

impl MarketEvent {
    /// Encodes exact integer values as strings for transport across JSON consumers.
    #[must_use]
    pub fn to_wire(&self) -> Value {
        let observation = match &self.observation {
            MarketObservation::Spot(spot) => json!({
                "kind": "spot", "account_id": spot.account_id.to_string(),
                "symbol_id": spot.symbol_id.to_string(),
                "bid": spot.bid.map(|v| v.to_string()), "ask": spot.ask.map(|v| v.to_string()),
                "broker_timestamp_ms": spot.broker_timestamp_ms.map(|v| v.to_string()),
                "received_utc_ns": spot.received_utc_ns.to_string(),
                "received_mono_ns": spot.received_mono_ns.to_string(),
                "price_scale": 100000
            }),
            MarketObservation::Depth(depth) => json!({
                "kind": "depth", "account_id": depth.account_id.to_string(),
                "symbol_id": depth.symbol_id.to_string(),
                "bids": depth.bids.iter().map(|(p, q)| [p.to_string(), q.to_string()]).collect::<Vec<_>>(),
                "asks": depth.asks.iter().map(|(p, q)| [p.to_string(), q.to_string()]).collect::<Vec<_>>(),
                "received_utc_ns": depth.received_utc_ns.to_string(),
                "received_mono_ns": depth.received_mono_ns.to_string(),
                "source_ts_ns": depth.source_ts_ns.map(|v| v.to_string()),
                "complete": depth.complete, "price_scale": 100000, "size_scale": 100
            }),
        };
        json!({"schema_version":"leafquant.ctrader-market.v1", "source":"ctrader-openapi",
            "observation":observation, "parsed_payload":self.raw_payload})
    }
}

/// Explicit source limits; queued wire bytes are not a process-memory guarantee.
#[derive(Debug, Clone, Copy)]
pub struct MarketStreamLimits {
    pub max_symbols: usize,
    pub max_entries_per_symbol: usize,
    pub max_levels: usize,
    pub event_capacity: usize,
    pub max_event_wire_bytes: usize,
    pub max_queued_wire_bytes: usize,
}

impl MarketStreamLimits {
    /// Validates bounded storage and queue budgets before any connection.
    ///
    /// # Errors
    /// Returns an error if any bound is invalid or internally inconsistent.
    pub fn validate(self) -> Result<()> {
        ensure!(
            (1..=1000).contains(&self.max_symbols),
            "Invalid depth symbol bound"
        );
        ensure!(
            (1..=100_000).contains(&self.max_entries_per_symbol),
            "Invalid depth entry bound"
        );
        ensure!(
            (1..=1000).contains(&self.max_levels) && self.max_levels <= self.max_entries_per_symbol,
            "Invalid depth level bound"
        );
        ensure!(
            self.max_symbols
                .checked_mul(self.max_entries_per_symbol)
                .is_some_and(|total| total <= 100_000),
            "Total depth entry bound exceeded"
        );
        ensure!(
            (1..=4096).contains(&self.event_capacity),
            "Invalid depth event capacity"
        );
        ensure!(
            (1024..=super::client::MAX_MESSAGE_BYTES).contains(&self.max_event_wire_bytes),
            "Invalid depth event wire bound"
        );
        ensure!(
            self.event_capacity
                .checked_mul(self.max_event_wire_bytes)
                .is_some_and(|total| total <= self.max_queued_wire_bytes),
            "Depth queue wire budget exceeded"
        );
        Ok(())
    }
}

/// A single consumer for spot and depth events on an explicitly isolated socket.
pub struct OpenApiMarketStream {
    lease: OpenApiLease,
    events: OpenApiEvents,
    account_id: u64,
    books: HashMap<u64, DepthBook>,
    verified_symbols: HashMap<u64, String>,
    raw_consumption: Option<bool>,
    max_symbols: usize,
    max_entries: usize,
    max_levels: usize,
    owner: Option<Arc<OpenApiAccountSession>>,
}

impl OpenApiMarketStream {
    /// Returns the authenticated broker account identity for binding checks.
    #[must_use]
    pub const fn account_id(&self) -> u64 {
        self.account_id
    }

    /// Creates a bounded stream without opening a connection or subscribing.
    ///
    /// # Errors
    /// Returns an error for a shared lease, unauthenticated account, or invalid bounds.
    pub(crate) fn new(
        mut lease: OpenApiLease,
        max_symbols: usize,
        max_entries: usize,
        max_levels: usize,
        max_event_wire_bytes: usize,
        max_queued_wire_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            lease.is_isolated(),
            "Depth stream requires an isolated session"
        );
        ensure!(
            (1..=1000).contains(&max_symbols),
            "Invalid depth symbol bound"
        );
        ensure!(
            (1..=100_000).contains(&max_entries),
            "Invalid depth entry bound"
        );
        ensure!(
            (1..=1000).contains(&max_levels),
            "Invalid depth level bound"
        );
        ensure!(
            max_levels <= max_entries,
            "Depth level bound exceeds entries"
        );
        ensure!(
            max_symbols
                .checked_mul(max_entries)
                .is_some_and(|total| total <= 100_000),
            "Total depth entry bound exceeded"
        );
        let account_id = lease.client().authenticated_account_id()?;
        lease
            .client()
            .validate_event_wire_budget(max_event_wire_bytes, max_queued_wire_bytes)?;
        let events = lease.take_events()?;
        Ok(Self {
            owner: None,
            lease,
            events,
            account_id,
            books: HashMap::new(),
            verified_symbols: HashMap::new(),
            raw_consumption: None,
            max_symbols,
            max_entries,
            max_levels,
        })
    }

    /// Acquires and retains an isolated socket using the existing account grant.
    ///
    /// # Errors
    /// Returns an error for failed authentication, connection, or invalid bounds.
    pub async fn connect(
        session: &OpenApiAccountSession,
        limits: MarketStreamLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let owner = Arc::new(
            session
                .isolated_peer_with_limits(limits.event_capacity, limits.max_event_wire_bytes)?,
        );
        let lease = owner.acquire().await?;
        Self::attach(
            owner,
            lease,
            limits.max_symbols,
            limits.max_entries_per_symbol,
            limits.max_levels,
            limits.max_event_wire_bytes,
            limits.max_queued_wire_bytes,
        )
        .await
    }

    pub(super) async fn attach(
        owner: Arc<OpenApiAccountSession>,
        lease: OpenApiLease,
        max_symbols: usize,
        max_entries: usize,
        max_levels: usize,
        max_event_wire_bytes: usize,
        max_queued_wire_bytes: usize,
    ) -> Result<Self> {
        ensure!(
            owner.owns_lease(&lease).await,
            "Depth session and lease differ"
        );
        let mut stream = Self::new(
            lease,
            max_symbols,
            max_entries,
            max_levels,
            max_event_wire_bytes,
            max_queued_wire_bytes,
        )?;
        stream.owner = Some(owner);
        Ok(stream)
    }

    /// Resolves configured names against the authenticated broker account.
    pub async fn resolve_symbols(&mut self, names: &[String]) -> Result<Vec<(String, u64)>> {
        ensure!(
            self.books.is_empty(),
            "Cannot resolve symbols after subscribing"
        );
        let payload = self
            .lease
            .client()
            .request(
                ReadOnlyRequest::Symbols,
                json!({"includeArchivedSymbols": false}),
            )
            .await?;
        let resolved =
            super::symbols::resolve(&payload, names, self.max_symbols).map_err(|e| anyhow!(e))?;
        self.verified_symbols = resolved
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();
        Ok(resolved)
    }

    /// Subscribes one broker symbol on the authenticated socket.
    ///
    /// # Errors
    /// Returns an error for duplicate, unbounded, or unsuccessful subscriptions.
    pub async fn subscribe(&mut self, symbol_id: u64) -> Result<()> {
        ensure!(symbol_id > 0, "Invalid depth symbol");
        ensure!(
            self.verified_symbols.contains_key(&symbol_id),
            "Unverified broker symbol"
        );
        ensure!(
            !self.books.contains_key(&symbol_id),
            "Duplicate depth subscription"
        );
        ensure!(
            self.books.len() < self.max_symbols,
            "Depth symbol bound exceeded"
        );
        self.books.insert(
            symbol_id,
            DepthBook::new(symbol_id, self.max_entries).map_err(|e| anyhow!(e))?,
        );
        for (request, payload) in [
            (
                ReadOnlyRequest::SubscribeSpots,
                json!({"symbolId":[symbol_id],"subscribeToSpotTimestamp":true}),
            ),
            (
                ReadOnlyRequest::SubscribeDepthQuotes,
                json!({"symbolId":[symbol_id]}),
            ),
        ] {
            if let Err(e) = self.lease.client().request(request, payload).await {
                // The broker may have accepted a request whose reply was lost.
                self.lease.client().request_close();
                return Err(e);
            }
        }
        Ok(())
    }

    /// Returns the next spot update or depth state, without fabricating missing fields.
    ///
    /// # Errors
    /// Returns an error and closes the epoch on missing, malformed, or foreign depth.
    pub async fn next(&mut self) -> Result<MarketEvent> {
        ensure!(
            self.raw_consumption != Some(true),
            "Cannot mix raw and reconstructed market events"
        );
        self.raw_consumption = Some(false);
        loop {
            let event = self
                .events
                .next()
                .await
                .context("Depth event stream ended")?;
            if event.payload_type == 2120 {
                self.lease.client().request_close();
                return Err(anyhow!("Broker symbol metadata changed"));
            }
            if !matches!(event.payload_type, 2131 | 2155) {
                continue;
            }
            let result = (|| -> Result<Option<MarketObservation>> {
                let symbol_id = required(&event.payload, "symbolId").map_err(|e| anyhow!(e))?;
                let book = self
                    .books
                    .get_mut(&symbol_id)
                    .context("Unsubscribed depth symbol")?;
                let received_utc_ns = u64::try_from(
                    event
                        .received_wall_time
                        .duration_since(UNIX_EPOCH)
                        .context("Invalid depth receive clock")?
                        .as_nanos(),
                )
                .context("Depth clock overflow")?;
                if event.payload_type == 2131 {
                    return Ok(decode_spot(
                        self.account_id,
                        symbol_id,
                        &event.payload,
                        received_utc_ns,
                        event.received_mono_ns,
                    )?
                    .map(MarketObservation::Spot));
                }
                apply_event(book, self.account_id, symbol_id, &event.payload)
                    .map_err(|e| anyhow!(e))?;
                let (bids, asks) = book.levels().map_err(|e| anyhow!(e))?;
                Ok(Some(MarketObservation::Depth(DepthObservation {
                    account_id: self.account_id,
                    symbol_id,
                    bids: bids.iter().take(self.max_levels).copied().collect(),
                    asks: asks.iter().take(self.max_levels).copied().collect(),
                    received_utc_ns,
                    received_mono_ns: event.received_mono_ns,
                    source_ts_ns: None,
                    complete: false,
                })))
            })();
            if result.is_err() {
                self.lease.client().request_close();
            }
            if let Some(observation) = result? {
                return Ok(MarketEvent {
                    observation,
                    raw_payload: event.payload,
                });
            }
        }
    }

    /// Returns the parsed broker event without retaining or reconstructing a depth book.
    ///
    /// # Errors
    /// Returns an error for mixed read modes, disconnected streams, or foreign identities.
    pub async fn next_raw(&mut self) -> Result<RawMarketEvent> {
        ensure!(
            self.raw_consumption != Some(false),
            "Cannot mix raw and reconstructed market events"
        );
        self.raw_consumption = Some(true);
        loop {
            let event = self
                .events
                .next()
                .await
                .context("Market event stream ended")?;
            if event.payload_type == 2120 {
                self.lease.client().request_close();
                return Err(anyhow!("Broker symbol metadata changed"));
            }
            if !matches!(event.payload_type, 2131 | 2155) {
                continue;
            }
            let result = (|| -> Result<RawMarketEvent> {
                let account_id =
                    required(&event.payload, "ctidTraderAccountId").map_err(|e| anyhow!(e))?;
                let symbol_id = required(&event.payload, "symbolId").map_err(|e| anyhow!(e))?;
                ensure!(
                    account_id == self.account_id && self.books.contains_key(&symbol_id),
                    "Foreign market identity"
                );
                let received_utc_ns = u64::try_from(
                    event
                        .received_wall_time
                        .duration_since(UNIX_EPOCH)?
                        .as_nanos(),
                )?;
                Ok(RawMarketEvent {
                    payload_type: event.payload_type,
                    account_id,
                    symbol_id,
                    received_utc_ns,
                    received_mono_ns: event.received_mono_ns,
                    raw_payload: event.payload,
                })
            })();
            if result.is_err() {
                self.lease.client().request_close();
            }
            return result;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_all_limits_before_connecting() {
        let valid = MarketStreamLimits {
            max_symbols: 2,
            max_entries_per_symbol: 100,
            max_levels: 10,
            event_capacity: 8,
            max_event_wire_bytes: 1024,
            max_queued_wire_bytes: 8192,
        };
        assert!(valid.validate().is_ok());
        assert!(
            MarketStreamLimits {
                max_queued_wire_bytes: 8191,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            MarketStreamLimits {
                max_symbols: 1000,
                ..valid
            }
            .validate()
            .is_ok()
        );
        assert!(
            MarketStreamLimits {
                max_symbols: 1000,
                max_entries_per_symbol: 101,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            MarketStreamLimits {
                event_capacity: 0,
                ..valid
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn spot_preserves_partial_values_and_missing_broker_time() {
        let event = decode_spot(
            42,
            7,
            &json!({"ctidTraderAccountId":"42", "symbolId":"7", "bid":"123456"}),
            10,
            20,
        )
        .unwrap()
        .unwrap();
        assert_eq!(event.bid, Some(123456));
        assert_eq!(event.ask, None);
        assert_eq!(event.broker_timestamp_ms, None);
        assert_eq!(event.received_utc_ns, 10);
        assert!(
            decode_spot(
                42,
                7,
                &json!({"ctidTraderAccountId":"43", "symbolId":"7", "ask":"123500"}),
                10,
                20
            )
            .is_err()
        );
    }
}
