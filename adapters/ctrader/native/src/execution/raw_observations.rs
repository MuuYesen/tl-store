// SPDX-License-Identifier: LGPL-3.0-only

//! Bounded original execution facts for non-interned AccountState metadata.

use crate::openapi::{OpenApiEvent, model::BrokerDeal};
use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{collections::VecDeque, io, time::UNIX_EPOCH};

const MAX_EVENT_BYTES: usize = 32 * 1024;
const MAX_BATCH_BYTES: usize = 64 * 1024;
const MAX_PENDING_BYTES: usize = 512 * 1024;
const MAX_PENDING_EVENTS: usize = 1024;

struct ByteLimit {
    bytes: usize,
    limit: usize,
}
impl io::Write for ByteLimit {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        if value.len() > self.limit.saturating_sub(self.bytes) {
            return Err(io::Error::other(
                "Original execution observation exceeds byte budget",
            ));
        }
        self.bytes += value.len();
        Ok(value.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encoded_bytes(value: &Value, limit: usize) -> Result<usize> {
    let mut counter = ByteLimit { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.bytes)
}

struct Entry {
    sequence: u64,
    bytes: usize,
    value: Value,
}
#[derive(Default)]
pub(crate) struct PendingExecutions {
    entries: VecDeque<Entry>,
    bytes: usize,
    sequence: u64,
}
pub(crate) struct Batch {
    pub through: u64,
    pub events: Vec<Value>,
}
impl PendingExecutions {
    /// Retains decoded original wire facts; no prices, fees or PnL are calculated.
    #[cfg(test)]
    pub(crate) fn record(&mut self, event: &OpenApiEvent, account: u64) -> Result<()> {
        self.record_bound(event, account, None)
    }
    pub(crate) fn record_bound(
        &mut self,
        event: &OpenApiEvent,
        account: u64,
        binding: Option<Value>,
    ) -> Result<()> {
        if event.payload_type != 2126 {
            return Ok(());
        }
        let Some(raw_deal) = event.payload.get("deal").filter(|v| !v.is_null()) else {
            return Ok(());
        };
        let account_id = event.payload["ctidTraderAccountId"]
            .as_u64()
            .or_else(|| event.payload["ctidTraderAccountId"].as_str()?.parse().ok());
        ensure!(
            account_id == Some(account),
            "Original execution account mismatch"
        );
        let deal = BrokerDeal::from_wire(raw_deal)?;
        if !matches!(deal.deal_status, 2 | 3) {
            return Ok(());
        }
        ensure!(
            deal.filled_volume > Decimal::ZERO,
            "Executed deal has no actual fill"
        );
        ensure!(
            event.received_mono_ns > 0,
            "Original receive clock is missing"
        );
        // Count before cloning, so an unexpectedly large event does not allocate another payload.
        encoded_bytes(&event.payload, MAX_EVENT_BYTES - 256)?;
        let received_utc_ns = u64::try_from(
            event
                .received_wall_time
                .duration_since(UNIX_EPOCH)?
                .as_nanos(),
        )?;
        let mut value = json!({"payload_type":event.payload_type,"payload":event.payload,
            "received_utc_ns":received_utc_ns,"received_mono_ns":event.received_mono_ns});
        if let Some(binding) = binding {
            encoded_bytes(&binding, MAX_EVENT_BYTES)?;
            value["request_binding"] = binding;
        }
        let bytes = encoded_bytes(&value, MAX_EVENT_BYTES)?;
        ensure!(
            self.entries.len() < MAX_PENDING_EVENTS
                && bytes <= MAX_PENDING_BYTES.saturating_sub(self.bytes),
            "Original execution observation queue is full"
        );
        let sequence = self
            .sequence
            .checked_add(1)
            .context("Observation sequence exhausted")?;
        self.entries.push_back(Entry {
            sequence,
            bytes,
            value,
        });
        self.sequence = sequence;
        self.bytes += bytes;
        Ok(())
    }

    pub(crate) fn batch(&self) -> Option<Batch> {
        let mut bytes = 2;
        let mut events = Vec::new();
        let mut through = None;
        for entry in &self.entries {
            if bytes + entry.bytes + 1 > MAX_BATCH_BYTES {
                break;
            }
            bytes += entry.bytes + 1;
            events.push(entry.value.clone());
            through = Some(entry.sequence);
        }
        through.map(|through| Batch { through, events })
    }

    /// Releases only the dispatched prefix; this is not a durable logger acknowledgement.
    pub(crate) fn acknowledge(&mut self, through: u64) {
        while self
            .entries
            .front()
            .is_some_and(|entry| entry.sequence <= through)
        {
            let entry = self
                .entries
                .pop_front()
                .expect("Checked original observation prefix");
            self.bytes -= entry.bytes;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::OpenApiEvent;
    use serde_json::json;
    use std::time::{Duration, Instant, UNIX_EPOCH};

    fn event(id: u64) -> OpenApiEvent {
        OpenApiEvent { client_message_id: None,
            payload_type: 2126,
            payload: json!({"ctidTraderAccountId":"9","executionType":3,
                "order": {"orderId":"42"},
                "deal":{"dealId":id,"orderId":"42","positionId":"77",
                    "symbolId":"7","volume":"1","filledVolume":"1",
                    "tradeSide":1,"dealStatus":2,"createTimestamp":"1000",
                    "executionTimestamp":"1001","executionPrice":"60000.25",
                    "commission":"-5","moneyDigits":2},"opaque":"original"}),
            received_at: Instant::now(),
            received_wall_time: UNIX_EPOCH + Duration::from_millis(1002),
            account_revision: 0,
            received_mono_ns: 2_000_000_000,
        }
    }

    #[test]
    fn original_facts_remain_until_dispatch_and_new_arrivals_are_not_acknowledged() {
        let mut pending = PendingExecutions::default();
        let first = event(11);
        pending.record(&first, 9).unwrap();
        let batch = pending.batch().unwrap();
        assert_eq!(batch.events[0]["payload"], first.payload);
        assert_eq!(batch.events[0]["received_utc_ns"], 1_002_000_000_u64);
        assert_eq!(pending.batch().unwrap().events, batch.events);
        pending.record(&event(12), 9).unwrap();
        pending.acknowledge(batch.through);
        let rest = pending.batch().unwrap();
        assert_eq!(rest.events.len(), 1);
        assert_eq!(rest.events[0]["payload"]["deal"]["dealId"], 12);
        pending.acknowledge(rest.through);
        assert!(pending.batch().is_none());
    }

    #[test]
    fn foreign_malformed_and_oversized_events_do_not_enter_the_queue() {
        let mut pending = PendingExecutions::default();
        assert!(pending.record(&event(11), 10).is_err());
        let mut invalid = event(11);
        invalid.payload["deal"]["filledVolume"] = json!("invalid");
        assert!(pending.record(&invalid, 9).is_err());
        let mut oversized = event(11);
        oversized.payload["opaque"] = json!("x".repeat(MAX_EVENT_BYTES));
        assert!(pending.record(&oversized, 9).is_err());
        assert!(pending.batch().is_none());
    }

    #[test]
    fn batch_and_pending_budgets_are_bounded_without_truncating_facts() {
        let mut pending = PendingExecutions::default();
        for id in 0..10 {
            let mut item = event(id + 1);
            item.payload["opaque"] = json!("x".repeat(MAX_EVENT_BYTES / 2));
            pending.record(&item, 9).unwrap();
        }
        let batch = pending.batch().unwrap();
        assert!(serde_json::to_vec(&batch.events).unwrap().len() <= MAX_BATCH_BYTES + 2);
        assert!(batch.events.len() < 10);
        let mut failed = false;
        for id in 20..200 {
            let mut item = event(id);
            item.payload["opaque"] = json!("x".repeat(MAX_EVENT_BYTES / 2));
            if pending.record(&item, 9).is_err() {
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert_eq!(pending.batch().unwrap().events, batch.events);
    }
}
