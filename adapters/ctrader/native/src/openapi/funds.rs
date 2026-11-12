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

//! Strict account-wide funds observations from broker PnL and margin evidence.
//!
//! The supported observation has explicit zero bonus/credit fields, stable pending orders,
//! stable complete account snapshots, and MAX or SUM margin calculation. Active NET
//! margin and nonzero/unknown bonus interpretations remain unsupported. This is a bounded
//! local observation, not an atomic server timestamp or an initial/maintenance margin report.
//!
//! Source contracts:
//! - <https://help.ctrader.com/open-api/profit-loss-calculation/>
//! - <https://help.ctrader.com/ctrader-algo/references/Account/IAccount/Equity/>
//! - <https://help.ctrader.com/ctrader-analyze/ctrader-web-analyze/>
//! - <https://help.ctrader.com/knowledge-base/glossary/trading/>
//! - <https://github.com/spotware/openapi-proto-messages/blob/main/OpenApiModelMessages.proto>

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::model::enum_field;

const MAX_POSITIONS: usize = 100_000;

/// Separate broker requests can observe different valid account states.
/// No financial values or freshness timestamp qualify from this sample.
#[derive(Debug)]
pub struct FundsObservationChanged;
impl std::fmt::Display for FundsObservationChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Funds account views changed during observation")
    }
}
impl std::error::Error for FundsObservationChanged {}

/// Exact account binding established by authenticated account and asset responses.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedFundsAccount {
    pub ctid_trader_account_id: u64,
    pub trader_login: u64,
    pub deposit_asset_id: u64,
}

/// Bounds sampled from the live lease and reader counter, never fabricated defaults.
///
/// `revision_*` comes from `OpenApiClient::account_revision`, `epoch_*` from the
/// retained lease, and all monotonic times from the same host CLOCK_MONOTONIC.
#[derive(Clone, Copy, Debug)]
pub struct FundsWindow {
    pub epoch_before: u64,
    pub epoch_after: u64,
    pub revision_before: u64,
    pub revision_after: u64,
    pub started_mono_ns: u64,
    pub completed_mono_ns: u64,
    pub now_mono_ns: u64,
    pub max_span_ns: u64,
    pub max_age_ns: u64,
    pub completed_utc_ns: u64,
}
impl FundsWindow {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.epoch_before > 0 && self.epoch_before == self.epoch_after,
            "Funds observation changed connection epoch"
        );
        ensure!(
            self.revision_before == self.revision_after,
            "Account changed during funds observation"
        );
        ensure!(
            self.started_mono_ns > 0
                && self.completed_utc_ns > 0
                && self.started_mono_ns <= self.completed_mono_ns
                && self.completed_mono_ns <= self.now_mono_ns,
            "Invalid funds observation clock bounds"
        );
        ensure!(
            self.max_span_ns > 0
                && self.max_age_ns > 0
                && self.completed_mono_ns - self.started_mono_ns <= self.max_span_ns
                && self.now_mono_ns - self.completed_mono_ns <= self.max_age_ns,
            "Funds observation is stale or spans too much time"
        );
        Ok(())
    }
}

/// Broker-derived equity and available margin, with raw balance retained separately.
#[derive(Clone, Debug)]
pub struct AccountFunds {
    pub balance: Decimal,
    pub net_unrealized_pnl: Decimal,
    pub equity: Decimal,
    pub used_margin: Decimal,
    pub free_margin: Decimal,
    pub observed_utc_ns: u64,
    pub epoch: u64,
    pub revision: u64,
    currency_code: String,
    account: ExpectedFundsAccount,
    started_mono_ns: u64,
    completed_mono_ns: u64,
}
impl AccountFunds {
    /// Validates complete account payloads around one broker-computed PnL response.
    ///
    /// The caller checks session health before and after collection and supplies the
    /// actual reader revisions. All positions are included, even outside strategy symbols.
    #[expect(clippy::too_many_arguments)]
    pub fn from_wire(
        expected: &ExpectedFundsAccount,
        window: &FundsWindow,
        trader_before: &Value,
        reconcile_before: &Value,
        pnl: &Value,
        reconcile_after: &Value,
        trader_after: &Value,
        assets: &Value,
    ) -> Result<Self> {
        window.validate()?;
        for identity in [
            expected.ctid_trader_account_id,
            expected.trader_login,
            expected.deposit_asset_id,
        ] {
            ensure!(
                identity > 0 && i64::try_from(identity).is_ok(),
                "Invalid funds account binding"
            );
        }
        for response in [
            trader_before,
            reconcile_before,
            pnl,
            reconcile_after,
            trader_after,
            assets,
        ] {
            ensure!(
                unsigned(response, "ctidTraderAccountId")? == expected.ctid_trader_account_id,
                "Funds response account identity mismatch"
            );
            ensure!(
                response.get("hasMore").is_none_or(|value| value == false),
                "Incomplete funds response"
            );
        }
        let before = trader(trader_before, expected)?;
        let after = trader(trader_after, expected)?;
        if before != after {
            return Err(FundsObservationChanged.into());
        }
        let money_digits = digits(before)?;
        let balance = signed_amount(before, "balance", money_digits)?;
        unsigned(before, "balanceVersion")?;
        // ProtoOATrader declares these as optional int64 fields in proto2,
        // where an absent numeric field reads as zero. Present null/invalid
        // values remain errors; nonzero bonus accounts remain unsupported.
        // https://github.com/spotware/openapi-proto-messages/blob/main/OpenApiModelMessages.proto
        for field in ["managerBonus", "ibBonus", "nonWithdrawableBonus"] {
            let bonus = match before.get(field) {
                None => 0,
                Some(_) => integer(before, field)?,
            };
            ensure!(
                bonus == 0,
                "Funds with nonzero bonus credit are unsupported"
            );
        }
        let mode = enum_field(
            before,
            "totalMarginCalculationType",
            &[("MAX", 0), ("SUM", 1), ("NET", 2)],
            Some(0), // Proto2 optional enum default: first value MAX
        )?;
        ensure!(mode <= 2, "Unknown total margin calculation type");
        // cTrader reserves margin for OPEN positions, not inactive pending orders.
        // https://help.ctrader.com/knowledge-base/glossary/trading/
        if pending_snapshot(reconcile_before)? != pending_snapshot(reconcile_after)? {
            return Err(FundsObservationChanged.into());
        }
        let positions = position_snapshot(reconcile_before)?;
        let after_positions = position_snapshot(reconcile_after)?;
        if positions != after_positions {
            return Err(FundsObservationChanged.into());
        }
        ensure!(
            mode != 2 || positions.is_empty(),
            "Active NET margin calculation is unsupported"
        );

        let mut per_symbol = HashMap::<u64, (Decimal, Decimal)>::new();
        for position in positions.values() {
            ensure!(
                enum_field(
                    position,
                    "positionStatus",
                    &[
                        ("POSITION_STATUS_OPEN", 1),
                        ("POSITION_STATUS_CLOSED", 2),
                        ("POSITION_STATUS_CREATED", 3),
                        ("POSITION_STATUS_ERROR", 4)
                    ],
                    None
                )? == 1,
                "Funds require open positions only"
            );
            let trade = position
                .get("tradeData")
                .context("Missing funds position tradeData")?;
            let symbol = positive_id(trade, "symbolId")?;
            ensure!(
                integer(trade, "volume")? > 0,
                "Invalid funds position volume"
            );
            let side = enum_field(trade, "tradeSide", &[("BUY", 1), ("SELL", 2)], None)?;
            ensure!(matches!(side, 1 | 2), "Invalid funds position side");
            let margin = Decimal::from_i128_with_scale(
                i128::from(unsigned(position, "usedMargin")?),
                digits(position)?,
            );
            let entry = per_symbol.entry(symbol).or_default();
            let bucket = if side == 1 {
                &mut entry.0
            } else {
                &mut entry.1
            };
            *bucket = exact_add(*bucket, margin)?;
        }
        let mut used_margin = Decimal::ZERO;
        for (long, short) in per_symbol.into_values() {
            let symbol_margin = if mode == 0 {
                long.max(short)
            } else {
                exact_add(long, short)?
            };
            used_margin = exact_add(used_margin, symbol_margin)?;
        }
        let pnl_digits = digits(pnl)?;
        let pnl_rows = repeated_rows(pnl, "positionUnrealizedPnL")?;
        ensure!(pnl_rows.len() <= MAX_POSITIONS, "Funds PnL bound exceeded");
        let mut seen = HashSet::new();
        let mut net_unrealized_pnl = Decimal::ZERO;
        for row in pnl_rows {
            let id = positive_id(row, "positionId")?;
            ensure!(seen.insert(id), "Duplicate funds PnL position");
            net_unrealized_pnl = exact_add(
                net_unrealized_pnl,
                signed_amount(row, "netUnrealizedPnL", pnl_digits)?,
            )?;
        }
        if seen.len() != positions.len() || !seen.iter().all(|id| positions.contains_key(id)) {
            return Err(FundsObservationChanged.into());
        }
        let equity = exact_add(balance, net_unrealized_pnl)?;
        let free_margin = exact_add(equity, -used_margin)?;
        let matching_assets = rows(assets, "asset")?
            .iter()
            .filter(|row| unsigned(row, "assetId").ok() == Some(expected.deposit_asset_id))
            .collect::<Vec<_>>();
        ensure!(
            matching_assets.len() == 1,
            "Missing or ambiguous funds deposit asset"
        );
        let currency_code = matching_assets[0]
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .context("Missing funds deposit currency")?
            .to_owned();
        Ok(Self {
            balance,
            net_unrealized_pnl,
            equity,
            used_margin,
            free_margin,
            observed_utc_ns: window.completed_utc_ns,
            epoch: window.epoch_after,
            revision: window.revision_after,
            currency_code,
            account: *expected,
            started_mono_ns: window.started_mono_ns,
            completed_mono_ns: window.completed_mono_ns,
        })
    }

    /// Projects exact broker equity and used margin to SDK currency precision.
    ///
    /// Exact domain values; the framework adapter owns fixed-point Money projection.
    pub fn to_json(&self) -> Value {
        json!({"schema_version":1,"account":{"ctid_trader_account_id":self.account.ctid_trader_account_id,
            "trader_login":self.account.trader_login,"deposit_asset_id":self.account.deposit_asset_id},
            "currency":self.currency_code,"epoch":self.epoch,"revision":self.revision,
            "observed_utc_ns":self.observed_utc_ns,"started_mono_ns":self.started_mono_ns,
            "completed_mono_ns":self.completed_mono_ns,"clock_scope":"local_observation_window",
            "raw":{"balance":self.balance.to_string(),"net_unrealized_pnl":self.net_unrealized_pnl.to_string(),
                "equity":self.equity.to_string(),"used_margin":self.used_margin.to_string(),"free_margin":self.free_margin.to_string()}})
    }
}

fn trader<'a>(response: &'a Value, expected: &ExpectedFundsAccount) -> Result<&'a Value> {
    let trader = response.get("trader").context("Missing funds Trader")?;
    ensure!(
        positive_id(trader, "ctidTraderAccountId")? == expected.ctid_trader_account_id
            && positive_id(trader, "traderLogin")? == expected.trader_login
            && positive_id(trader, "depositAssetId")? == expected.deposit_asset_id,
        "Funds Trader identity mismatch"
    );
    ensure!(
        !trader.get("accountType").is_some_and(Value::is_null),
        "Invalid funds accountType"
    );
    ensure!(
        enum_field(
            trader,
            "accountType",
            &[("HEDGED", 0), ("NETTED", 1), ("SPREAD_BETTING", 2)],
            Some(0)
        )? == 0,
        "Funds require a hedged account"
    );
    Ok(trader)
}
fn pending_snapshot(response: &Value) -> Result<BTreeMap<u64, Value>> {
    let rows = repeated_rows(response, "order")?;
    ensure!(
        rows.len() <= MAX_POSITIONS,
        "Funds pending order bound exceeded"
    );
    let mut result = BTreeMap::new();
    for row in rows {
        let kind = enum_field(
            row,
            "orderType",
            &[
                ("MARKET", 1),
                ("LIMIT", 2),
                ("STOP", 3),
                ("STOP_LOSS_TAKE_PROFIT", 4),
                ("MARKET_RANGE", 5),
                ("STOP_LIMIT", 6),
            ],
            None,
        )?;
        let status = enum_field(
            row,
            "orderStatus",
            &[
                ("ORDER_STATUS_ACCEPTED", 1),
                ("ORDER_STATUS_FILLED", 2),
                ("ORDER_STATUS_REJECTED", 3),
                ("ORDER_STATUS_EXPIRED", 4),
                ("ORDER_STATUS_CANCELLED", 5),
            ],
            None,
        )?;
        ensure!(
            (1..=6).contains(&kind) && status == 1,
            "Funds require known accepted pending orders"
        );
        let trade = row
            .get("tradeData")
            .context("Missing funds pending tradeData")?;
        positive_id(trade, "symbolId")?;
        ensure!(
            integer(trade, "volume")? > 0,
            "Invalid funds pending volume"
        );
        ensure!(
            matches!(
                enum_field(trade, "tradeSide", &[("BUY", 1), ("SELL", 2)], None)?,
                1 | 2
            ),
            "Invalid funds pending side"
        );
        ensure!(
            result
                .insert(positive_id(row, "orderId")?, row.clone())
                .is_none(),
            "Duplicate funds pending order"
        );
    }
    Ok(result)
}
fn position_snapshot(response: &Value) -> Result<BTreeMap<u64, Value>> {
    let rows = repeated_rows(response, "position")?;
    ensure!(rows.len() <= MAX_POSITIONS, "Funds position bound exceeded");
    let mut result = BTreeMap::new();
    for row in rows {
        ensure!(
            result
                .insert(positive_id(row, "positionId")?, row.clone())
                .is_none(),
            "Duplicate funds position"
        );
    }
    Ok(result)
}
// Repeated protobuf fields may be omitted when empty. Explicit malformed JSON
// is rejected; the position/PnL identity checks still require an exact match.
fn repeated_rows<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    match value.get(field) {
        None => Ok(&[]),
        Some(value) => value
            .as_array()
            .map(Vec::as_slice)
            .with_context(|| format!("Invalid funds {field} array")),
    }
}
fn rows<'a>(value: &'a Value, field: &str) -> Result<&'a Vec<Value>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("Missing funds {field}"))
}
fn unsigned(value: &Value, field: &str) -> Result<u64> {
    let value = value
        .get(field)
        .with_context(|| format!("Missing funds {field}"))?;
    match value {
        Value::Number(number) => number.as_u64().context("Invalid funds unsigned integer"),
        Value::String(text) => text.parse().context("Invalid funds unsigned integer"),
        _ => anyhow::bail!("Invalid funds unsigned integer"),
    }
}
fn integer(value: &Value, field: &str) -> Result<i64> {
    let value = value
        .get(field)
        .with_context(|| format!("Missing funds {field}"))?;
    match value {
        Value::Number(number) => number.as_i64().context("Invalid funds signed integer"),
        Value::String(text) => text.parse().context("Invalid funds signed integer"),
        _ => anyhow::bail!("Invalid funds signed integer"),
    }
}
fn positive_id(value: &Value, field: &str) -> Result<u64> {
    let id = unsigned(value, field)?;
    ensure!(
        id > 0 && i64::try_from(id).is_ok(),
        "Invalid funds identity"
    );
    Ok(id)
}
fn digits(value: &Value) -> Result<u32> {
    let digits = u32::try_from(unsigned(value, "moneyDigits")?)?;
    ensure!(digits <= 28, "Unsupported funds moneyDigits");
    Ok(digits)
}
fn signed_amount(value: &Value, field: &str, digits: u32) -> Result<Decimal> {
    crate::openapi::model::amount_from_wire(integer(value, field)?, digits)
}
pub(crate) fn exact_add(left: Decimal, right: Decimal) -> Result<Decimal> {
    // Decimal arithmetic may reduce scale to fit; align checked integer mantissas instead
    let left = left.normalize();
    let right = right.normalize();
    let mut scale = left.scale().max(right.scale());
    let align = |value: Decimal| {
        value
            .mantissa()
            .checked_mul(10_i128.checked_pow(scale - value.scale())?)
    };
    let mut sum = align(left)
        .and_then(|left| align(right).and_then(|right| left.checked_add(right)))
        .context("Funds exact arithmetic exceeds integer bounds")?;
    while scale > 0 && sum % 10 == 0 {
        sum /= 10;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(sum, scale)
        .context("Funds exact arithmetic exceeds Decimal precision")
}

#[cfg(test)]
mod pending_funds_tests {
    use super::*;
    use rstest::rstest;
    fn observe(orders: Value, after: Option<Value>) -> Result<AccountFunds> {
        let expected = ExpectedFundsAccount {
            ctid_trader_account_id: 42,
            trader_login: 12345,
            deposit_asset_id: 2,
        };
        let window = FundsWindow {
            epoch_before: 1,
            epoch_after: 1,
            revision_before: 7,
            revision_after: 7,
            started_mono_ns: 100,
            completed_mono_ns: 200,
            now_mono_ns: 210,
            max_span_ns: 150,
            max_age_ns: 100,
            completed_utc_ns: 1000000,
        };
        let trader = json!({"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42","traderLogin":"12345","depositAssetId":"2","accountType":0,"moneyDigits":2,"balance":"10000","balanceVersion":"10","totalMarginCalculationType":0}});
        let position = json!({"positionId":"1","positionStatus":1,"moneyDigits":2,"usedMargin":"4000","tradeData":{"symbolId":"7","tradeSide":1,"volume":"100"}});
        let before = json!({"ctidTraderAccountId":"42","position":[position],"order":orders});
        let mut other = before.clone();
        if let Some(after) = after {
            other["order"] = after;
        }
        let pnl = json!({"ctidTraderAccountId":"42","moneyDigits":2,"positionUnrealizedPnL":[{"positionId":"1","netUnrealizedPnL":"200"}]});
        let assets =
            json!({"ctidTraderAccountId":"42","asset":[{"assetId":"2","name":"USD","digits":2}]});
        AccountFunds::from_wire(
            &expected, &window, &trader, &before, &pnl, &other, &trader, &assets,
        )
    }
    fn pending() -> Value {
        json!({"orderId":"99","orderType":2,"orderStatus":1,"tradeData":{"symbolId":"7","tradeSide":1,"volume":"100"},"limitPrice":1.0})
    }
    #[rstest]
    fn stable_pending_orders_do_not_reserve_open_position_margin() {
        let result = observe(json!([pending()]), None).unwrap();
        assert_eq!(result.equity, Decimal::new(102, 0));
        assert_eq!(result.used_margin, Decimal::new(40, 0));
        assert_eq!(result.free_margin, Decimal::new(62, 0));
    }
    #[rstest]
    fn unknown_or_executed_pending_order_is_not_an_inactive_order() {
        let mut bad = pending();
        bad["orderType"] = json!(99);
        assert!(observe(json!([bad]), None).is_err());
        let mut bad = pending();
        bad["orderStatus"] = json!(2);
        assert!(observe(json!([bad]), None).is_err());
    }

    #[rstest]
    fn changed_or_duplicate_or_malformed_pending_orders_fail_closed() {
        assert!(observe(json!([pending()]), Some(json!([]))).is_err());
        assert!(observe(json!([pending(), pending()]), None).is_err());
        assert!(observe(json!([{"orderId":"99"}]), None).is_err());
        assert!(observe(Value::Null, None).is_err());
    }
}

#[cfg(test)]
mod sample_alignment_r81_tests {
    use super::*;
    fn replay(pnl_ids: &[u64]) -> Result<AccountFunds> {
        let account = ExpectedFundsAccount {
            ctid_trader_account_id: 42,
            trader_login: 12345,
            deposit_asset_id: 2,
        };
        let window = FundsWindow {
            epoch_before: 1,
            epoch_after: 1,
            revision_before: 7,
            revision_after: 7,
            started_mono_ns: 100,
            completed_mono_ns: 200,
            now_mono_ns: 210,
            max_span_ns: 150,
            max_age_ns: 100,
            completed_utc_ns: 1_000_000,
        };
        let trader = json!({"ctidTraderAccountId":"42","trader":{"ctidTraderAccountId":"42","traderLogin":"12345","depositAssetId":"2","accountType":0,"moneyDigits":2,"balance":"10000","balanceVersion":"10","totalMarginCalculationType":0}});
        let reconcile = json!({"ctidTraderAccountId":"42","position":[{"positionId":"1","positionStatus":1,"moneyDigits":2,"usedMargin":"4000","tradeData":{"symbolId":"7","tradeSide":1,"volume":"100"}}],"order":[]});
        let rows: Vec<Value> = pnl_ids
            .iter()
            .map(|id| json!({"positionId":id.to_string(),"netUnrealizedPnL":"200"}))
            .collect();
        let pnl = json!({"ctidTraderAccountId":"42","moneyDigits":2,"positionUnrealizedPnL":rows});
        let assets =
            json!({"ctidTraderAccountId":"42","asset":[{"assetId":"2","name":"USD","digits":2}]});
        AccountFunds::from_wire(
            &account, &window, &trader, &reconcile, &pnl, &reconcile, &trader, &assets,
        )
    }
    #[test]
    fn valid_sample_still_has_exact_funds() {
        let v = replay(&[1]).unwrap();
        assert_eq!(v.equity, Decimal::new(102, 0));
        assert_eq!(v.free_margin, Decimal::new(62, 0));
    }
    #[test]
    fn different_pnl_position_set_is_unqualified_not_financials() {
        let e = replay(&[2]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Funds account views changed during observation"
        );
    }
    #[test]
    fn missing_pnl_set_is_unqualified_not_financials() {
        let e = replay(&[]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "Funds account views changed during observation"
        );
    }
    #[test]
    fn duplicate_after_unexpected_position_remains_hard_failure() {
        let e = replay(&[2, 2]).unwrap_err();
        assert_eq!(e.to_string(), "Duplicate funds PnL position");
        assert!(e.downcast_ref::<FundsObservationChanged>().is_none());
    }
    #[test]
    fn membership_difference_has_typed_invalidation() {
        assert!(
            replay(&[2])
                .unwrap_err()
                .downcast_ref::<FundsObservationChanged>()
                .is_some()
        );
    }
    #[test]
    fn duplicate_pnl_remains_a_hard_failure() {
        let e = replay(&[1, 1]).unwrap_err();
        assert_eq!(e.to_string(), "Duplicate funds PnL position");
    }
}
