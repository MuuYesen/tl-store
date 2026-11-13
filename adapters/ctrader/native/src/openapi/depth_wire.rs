// SPDX-License-Identifier: LGPL-3.0-only
//! Authenticated Open API depth event decoding.

use super::depth::{DepthBook, Quote, Side};
use serde_json::Value;

fn unsigned(value: &Value) -> Result<u64, &'static str> {
    let value = match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) if !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit()) => {
            value.parse().ok()
        }
        _ => None,
    };
    value.ok_or("invalid depth integer")
}

pub(super) fn positive(value: &Value) -> Result<u64, &'static str> {
    let value = unsigned(value)?;
    if value > 0 {
        Ok(value)
    } else {
        Err("invalid depth integer")
    }
}

pub(super) fn required(payload: &Value, field: &str) -> Result<u64, &'static str> {
    positive(payload.get(field).ok_or("missing depth field")?)
}

/// Decodes one broker event and applies it atomically to its connection epoch.
pub(crate) fn apply_event(
    book: &mut DepthBook,
    account_id: u64,
    symbol_id: u64,
    payload: &Value,
) -> Result<(), &'static str> {
    let result = (|| {
        if account_id == 0
            || symbol_id == 0
            || required(payload, "ctidTraderAccountId")? != account_id
            || required(payload, "symbolId")? != symbol_id
        {
            return Err("depth account or symbol mismatch");
        }
        let empty = Vec::new();
        let array = |name: &str| -> Result<&Vec<Value>, &'static str> {
            match payload.get(name) {
                Some(Value::Array(items)) if items.len() <= book.capacity() => Ok(items),
                None => Ok(&empty),
                _ => Err("invalid depth array"),
            }
        };
        let mut quotes = Vec::new();
        for item in array("newQuotes")? {
            let (side, price) = match (item.get("bid"), item.get("ask")) {
                (Some(bid), None) => (Side::Bid, positive(bid)?),
                (None, Some(ask)) => (Side::Ask, positive(ask)?),
                _ => return Err("depth quote requires exactly one side"),
            };
            quotes.push(Quote {
                id: required(item, "id")?,
                side,
                price,
                size: unsigned(item.get("size").ok_or("missing depth field")?)?,
            });
        }
        let deleted = array("deletedQuotes")?
            .iter()
            .map(positive)
            .collect::<Result<Vec<_>, _>>()?;
        book.apply(symbol_id, &quotes, &deleted)
    })();
    if result.is_err() {
        book.invalidate();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_exact_protocol_values_and_applies_deletions() {
        let mut book = DepthBook::new(7, 4).unwrap();
        apply_event(
            &mut book,
            42,
            7,
            &json!({"ctidTraderAccountId":"42", "symbolId":"7",
            "newQuotes":[{"id":"1", "size":"100", "bid":"123456"},
                         {"id":"2", "size":"50", "ask":"123500"}]}),
        )
        .unwrap();
        assert_eq!(book.levels().unwrap().0, &[(123_456, 100)]);
        apply_event(
            &mut book,
            42,
            7,
            &json!({"ctidTraderAccountId":42, "symbolId":7,
            "deletedQuotes":["1"]}),
        )
        .unwrap();
        assert!(book.levels().unwrap().0.is_empty());
    }

    #[test]
    fn unsigned_size_zero_is_not_a_deletion_or_liquidity() {
        for size in [json!(0), json!("0")] {
            let mut book = DepthBook::new(7, 1).unwrap();
            apply_event(
                &mut book,
                42,
                7,
                &json!({
                    "ctidTraderAccountId":42,"symbolId":7,
                    "newQuotes":[{"id":1,"bid":100,"size":size}]
                }),
            )
            .unwrap();
            assert!(book.levels().unwrap().0.is_empty());
            assert!(apply_event(
                &mut book,
                42,
                7,
                &json!({
                    "ctidTraderAccountId":42,"symbolId":7,
                    "newQuotes":[{"id":2,"bid":99,"size":1}]
                })
            )
            .is_err());
        }
        for size in [
            json!(-1),
            json!(true),
            json!(1.5),
            json!("18446744073709551616"),
            json!("-1"),
        ] {
            let mut book = DepthBook::new(7, 1).unwrap();
            assert!(apply_event(
                &mut book,
                42,
                7,
                &json!({
                    "ctidTraderAccountId":42,"symbolId":7,
                    "newQuotes":[{"id":1,"bid":100,"size":size}]
                })
            )
            .is_err());
            assert!(book.levels().is_err());
        }
    }

    #[test]
    fn rejects_invalid_identity_or_ambiguous_quote_and_poison_epoch() {
        let mut book = DepthBook::new(7, 4).unwrap();
        let invalid = json!({"ctidTraderAccountId":"43", "symbolId":"7",
            "newQuotes":[{"id":"1", "size":"100", "bid":"123456"} ]});
        assert!(apply_event(&mut book, 42, 7, &invalid).is_err());
        assert!(book.levels().is_err());
        let valid = json!({"ctidTraderAccountId":"42", "symbolId":"7",
            "newQuotes":[{"id":"1", "size":"100", "bid":"123456"} ]});
        assert!(apply_event(&mut book, 42, 7, &valid).is_err());
        let mut book = DepthBook::new(7, 4).unwrap();
        let ambiguous = json!({"ctidTraderAccountId":"42", "symbolId":"7",
            "newQuotes":[{"id":"1", "size":"100", "bid":"123456", "ask":"123500"}]});
        assert!(apply_event(&mut book, 42, 7, &ambiguous).is_err());
        assert!(book.levels().is_err());
    }

    #[test]
    fn rejects_over_capacity_zero_identity_and_noninteger_wire_values() {
        for payload in [
            json!({"ctidTraderAccountId":42, "symbolId":7,
                "newQuotes":[{"id":1,"size":100,"bid":1},{"id":2,"size":100,"bid":1}]}),
            json!({"ctidTraderAccountId":42, "symbolId":7,
                "newQuotes":[{"id":0,"size":100,"bid":1}]}),
            json!({"ctidTraderAccountId":42, "symbolId":7,
                "newQuotes":[{"id":true,"size":100,"bid":1}]}),
            json!({"ctidTraderAccountId":42, "symbolId":7,
                "newQuotes":[{"id":1,"size":100,"bid":1.5}]}),
        ] {
            let mut book = DepthBook::new(7, 1).unwrap();
            assert!(apply_event(&mut book, 42, 7, &payload).is_err());
            assert!(book.levels().is_err());
        }
    }
}
