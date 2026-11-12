// SPDX-License-Identifier: LGPL-3.0-only
//! Explicit broker symbol identity resolution for market subscriptions.

use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(super) fn resolve(
    payload: &Value,
    names: &[String],
    max: usize,
) -> Result<Vec<(String, u64)>, &'static str> {
    if names.is_empty() || names.len() > max {
        return Err("Invalid symbol count");
    }
    let mut requested = HashSet::new();
    for name in names {
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
            || !requested.insert(name.as_str())
        {
            return Err("Invalid or duplicate symbol name");
        }
    }
    let rows = payload
        .get("symbol")
        .and_then(Value::as_array)
        .ok_or("Missing broker symbols")?;
    let mut found = HashMap::new();
    let mut ids = HashSet::new();
    for row in rows {
        let Some(name) = row.get("symbolName").and_then(Value::as_str) else {
            continue;
        };
        if !requested.contains(name) {
            continue;
        }
        let value = row.get("symbolId").ok_or("Missing symbol ID")?;
        let id = value
            .as_u64()
            .or_else(|| {
                value.as_str().and_then(|s| {
                    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
                        None
                    } else {
                        s.parse().ok()
                    }
                })
            })
            .filter(|id| *id > 0)
            .ok_or("Invalid symbol ID")?;
        if found.insert(name, id).is_some() || !ids.insert(id) {
            return Err("Ambiguous broker symbols");
        }
    }
    names
        .iter()
        .map(|name| {
            found
                .get(name.as_str())
                .copied()
                .map(|id| (name.clone(), id))
                .ok_or("Broker symbol not found")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_only_requested_exact_symbols_and_rejects_ambiguity() {
        let payload = json!({"symbol":[{"symbolName":"BTCUSD","symbolId":"7"},
            {"symbolName":"XAUUSD","symbolId":"8"}, {"symbolName":"OTHER","symbolId":"9"}]});
        let names = vec!["XAUUSD".to_owned(), "BTCUSD".to_owned()];
        assert_eq!(
            resolve(&payload, &names, 2).unwrap(),
            vec![("XAUUSD".to_owned(), 8), ("BTCUSD".to_owned(), 7)]
        );
        assert!(resolve(&payload, &["MISSING".to_owned()], 2).is_err());
        assert!(resolve(&payload, &["BTCUSD".to_owned(), "BTCUSD".to_owned()], 2).is_err());
        let ambiguous = json!({"symbol":[{"symbolName":"BTCUSD","symbolId":"7"},
            {"symbolName":"BTCUSD","symbolId":"8"}]});
        assert!(resolve(&ambiguous, &["BTCUSD".to_owned()], 2).is_err());
    }
}
