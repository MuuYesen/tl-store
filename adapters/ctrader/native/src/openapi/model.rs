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

//! Exact account and deal values decoded from cTrader Open API JSON.

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use std::str::FromStr;

fn integer(value: &Value, field: &str) -> Result<i64> {
    let raw = value
        .get(field)
        .with_context(|| format!("missing {field}"))?;
    match raw {
        Value::Number(value) => value.as_i64().with_context(|| format!("invalid {field}")),
        Value::String(value) => value.parse().with_context(|| format!("invalid {field}")),
        _ => bail!("invalid {field}"),
    }
}

fn unsigned(value: &Value, field: &str) -> Result<u64> {
    let raw = value
        .get(field)
        .with_context(|| format!("missing {field}"))?;
    match raw {
        Value::Number(value) => value.as_u64().with_context(|| format!("invalid {field}")),
        Value::String(value) => value.parse().with_context(|| format!("invalid {field}")),
        _ => bail!("invalid {field}"),
    }
}

fn optional_integer(value: &Value, field: &str) -> Result<Option<i64>> {
    if value.get(field).is_none_or(Value::is_null) {
        Ok(None)
    } else {
        integer(value, field).map(Some)
    }
}

fn optional_unsigned(value: &Value, field: &str) -> Result<Option<u64>> {
    if value.get(field).is_none_or(Value::is_null) {
        Ok(None)
    } else {
        unsigned(value, field).map(Some)
    }
}

/// Decodes only enumerators explicitly defined by the broker protocol, in JSON name or numeric form.
pub(crate) fn enum_field(
    value: &Value,
    field: &str,
    variants: &[(&str, u32)],
    default: Option<u32>,
) -> Result<u32> {
    let Some(raw) = value.get(field).filter(|v| !v.is_null()) else {
        return default.with_context(|| format!("Missing {field}"));
    };
    let parsed = match raw {
        Value::String(name) => variants
            .iter()
            .find(|(known, _)| *known == name)
            .map(|(_, id)| *id)
            .or_else(|| name.parse::<u32>().ok()),
        Value::Number(n) => n.as_u64().and_then(|id| u32::try_from(id).ok()),
        _ => None,
    }
    .with_context(|| format!("Invalid {field} enum"))?;
    anyhow::ensure!(
        variants.iter().any(|(_, id)| *id == parsed),
        "Unknown {field} enum"
    );
    Ok(parsed)
}
fn optional_decimal(value: &Value, field: &str) -> Result<Option<Decimal>> {
    let Some(raw) = value.get(field).filter(|raw| !raw.is_null()) else {
        return Ok(None);
    };
    let text = match raw {
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        _ => bail!("invalid {field}"),
    };
    Decimal::from_str(&text)
        .map(Some)
        .with_context(|| format!("invalid {field}"))
}

/// Converts integer wire money to its business value without floating-point rounding.
pub fn amount_from_wire(amount: i64, money_digits: u32) -> Result<Decimal> {
    if money_digits > 28 {
        bail!("moneyDigits exceeds Decimal precision");
    }
    Ok(Decimal::from_i128_with_scale(
        i128::from(amount),
        money_digits,
    ))
}

/// Converts cTrader volume, which is expressed in hundredths of a unit.
pub fn volume_from_wire(volume: i64) -> Decimal {
    Decimal::from_i128_with_scale(i128::from(volume), 2)
}

/// A broker account balance observation; unavailable derived fields remain absent.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BrokerAccountSnapshot {
    pub ctid_trader_account_id: u64,
    pub trader_login: u64,
    pub deposit_asset_id: u64,
    pub balance: Decimal,
    pub equity: Option<Decimal>,
    pub money_digits: u32,
}

impl BrokerAccountSnapshot {
    /// Decodes `ProtoOATrader` without inventing equity from a balance-only payload.
    pub fn from_trader(trader: &Value) -> Result<Self> {
        let money_digits = u32::try_from(unsigned(trader, "moneyDigits")?)?;
        Ok(Self {
            ctid_trader_account_id: unsigned(trader, "ctidTraderAccountId")?,
            trader_login: unsigned(trader, "traderLogin")?,
            deposit_asset_id: unsigned(trader, "depositAssetId")?,
            balance: amount_from_wire(integer(trader, "balance")?, money_digits)?,
            equity: None,
            money_digits,
        })
    }
}

/// A broker deal retaining identifiers, exact fee provenance, and broker timestamps.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BrokerDeal {
    pub deal_id: u64,
    pub order_id: u64,
    pub position_id: u64,
    pub symbol_id: u64,
    pub requested_volume: Decimal,
    pub filled_volume: Decimal,
    pub trade_side: u32,
    pub deal_status: u32,
    pub create_timestamp_ms: i64,
    pub execution_timestamp_ms: i64,
    pub execution_price: Option<Decimal>,
    pub commission: Option<Decimal>,
    pub raw_commission: Option<i64>,
    pub money_digits: Option<u32>,
    /// Broker close accounting retained separately; never blindly added to deal commission.
    pub close_position_detail: Option<Value>,
}

impl BrokerDeal {
    /// Decodes `ProtoOADeal`; a missing commission is unknown rather than zero.
    pub fn from_wire(deal: &Value) -> Result<Self> {
        let raw_commission = optional_integer(deal, "commission")?;
        let money_digits = optional_unsigned(deal, "moneyDigits")?
            .map(u32::try_from)
            .transpose()?;
        let commission = match (raw_commission, money_digits) {
            (Some(value), Some(digits)) => Some(amount_from_wire(value, digits)?),
            _ => None,
        };
        let trade_side = enum_field(deal, "tradeSide", &[("BUY", 1), ("SELL", 2)], None)?;
        let deal_status = enum_field(
            deal,
            "dealStatus",
            &[
                ("FILLED", 2),
                ("PARTIALLY_FILLED", 3),
                ("REJECTED", 4),
                ("INTERNALLY_REJECTED", 5),
                ("ERROR", 6),
                ("MISSED", 7),
            ],
            None,
        )?;
        let requested_volume = volume_from_wire(integer(deal, "volume")?);
        let filled_volume = volume_from_wire(integer(deal, "filledVolume")?);
        anyhow::ensure!(
            matches!(trade_side, 1 | 2) && (2..=7).contains(&deal_status),
            "Unknown broker deal side or status"
        );
        anyhow::ensure!(
            requested_volume > Decimal::ZERO
                && filled_volume >= Decimal::ZERO
                && filled_volume <= requested_volume,
            "Invalid broker deal volume"
        );
        Ok(Self {
            deal_id: positive_id(deal, "dealId")?,
            order_id: positive_id(deal, "orderId")?,
            position_id: positive_id(deal, "positionId")?,
            symbol_id: positive_id(deal, "symbolId")?,
            requested_volume,
            filled_volume,
            trade_side,
            deal_status,
            create_timestamp_ms: integer(deal, "createTimestamp")?,
            execution_timestamp_ms: integer(deal, "executionTimestamp")?,
            execution_price: optional_decimal(deal, "executionPrice")?,
            commission,
            raw_commission,
            money_digits,
            close_position_detail: deal
                .get("closePositionDetail")
                .filter(|v| !v.is_null())
                .cloned(),
        })
    }

    /// Returns the broker's create-to-execute interval for a chronologically valid deal.
    pub fn broker_execution_latency_ms(&self) -> Option<i64> {
        self.execution_timestamp_ms
            .checked_sub(self.create_timestamp_ms)
            .filter(|duration| *duration >= 0)
    }
}

/// Explicit broker account mode and rights. Omitted protobuf defaults are not execution evidence.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BrokerTrader {
    pub account: BrokerAccountSnapshot,
    pub account_type: u32,
    pub access_rights: u32,
}
impl BrokerTrader {
    pub fn from_wire(value: &Value) -> Result<Self> {
        let account_type = enum_field(
            value,
            "accountType",
            &[("HEDGED", 0), ("NETTED", 1), ("SPREAD_BETTING", 2)],
            None,
        )?;
        let access_rights = enum_field(
            value,
            "accessRights",
            &[
                ("FULL_ACCESS", 0),
                ("CLOSE_ONLY", 1),
                ("NO_TRADING", 2),
                ("NO_LOGIN", 3),
            ],
            None,
        )?;
        anyhow::ensure!(
            account_type <= 2 && access_rights <= 3,
            "Unknown account mode or access rights"
        );
        Ok(Self {
            account: BrokerAccountSnapshot::from_trader(value)?,
            account_type,
            access_rights,
        })
    }
    pub fn is_hedged_full_access(&self) -> bool {
        self.account_type == 0 && self.access_rights == 0
    }
}

fn positive_id(value: &Value, field: &str) -> Result<u64> {
    let id = unsigned(value, field)?;
    anyhow::ensure!(id > 0, "Nonpositive {field}");
    Ok(id)
}
fn trade_identity(value: &Value) -> Result<(u64, Decimal, u32)> {
    let symbol = positive_id(value, "symbolId")?;
    let volume = volume_from_wire(integer(value, "volume")?);
    let side = enum_field(value, "tradeSide", &[("BUY", 1), ("SELL", 2)], None)?;
    anyhow::ensure!(
        volume >= Decimal::ZERO && matches!(side, 1 | 2),
        "Invalid trade identity"
    );
    Ok((symbol, volume, side))
}

/// Authoritative broker order, retaining quantities in underlying units.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BrokerOrder {
    pub order_id: u64,
    pub position_id: Option<u64>,
    pub symbol_id: u64,
    pub client_order_id: Option<String>,
    pub order_type: u32,
    pub order_status: u32,
    pub trade_side: u32,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub execution_price: Option<Decimal>,
    pub limit_price: Option<Decimal>,
    pub stop_price: Option<Decimal>,
    pub time_in_force: u32,
    pub expiration_timestamp_ms: Option<i64>,
    pub closing_order: Option<bool>,
    pub open_timestamp_ms: i64,
    pub update_timestamp_ms: i64,
}
impl BrokerOrder {
    pub fn from_wire(value: &Value) -> Result<Self> {
        let trade = value.get("tradeData").context("Missing order tradeData")?;
        let (symbol_id, quantity, trade_side) = trade_identity(trade)?;
        // Proto2 order acknowledgements may omit these optional scalars.
        // Zero clocks remain unknown until canonical OrderDetails supplies them.
        let open_timestamp_ms = optional_integer(trade, "openTimestamp")?.unwrap_or(0);
        anyhow::ensure!(quantity > Decimal::ZERO, "Order quantity must be positive");
        let filled_quantity =
            volume_from_wire(optional_integer(value, "executedVolume")?.unwrap_or(0));
        let order_type = enum_field(
            value,
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
        let order_status = enum_field(
            value,
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
        let update_timestamp_ms = optional_integer(value, "utcLastUpdateTimestamp")?.unwrap_or(0);
        anyhow::ensure!(
            (1..=6).contains(&order_type) && (1..=5).contains(&order_status),
            "Unknown order type or status"
        );
        anyhow::ensure!(
            filled_quantity >= Decimal::ZERO
                && filled_quantity <= quantity
                && open_timestamp_ms >= 0
                && update_timestamp_ms >= 0
                && (open_timestamp_ms == 0
                    || update_timestamp_ms == 0
                    || update_timestamp_ms >= open_timestamp_ms),
            "Invalid order quantities or timestamps"
        );
        // ProtoOAOrder.timeInForce explicitly defaults to IOC in the broker schema.
        let time_in_force = enum_field(
            value,
            "timeInForce",
            &[
                ("GOOD_TILL_DATE", 1),
                ("GOOD_TILL_CANCEL", 2),
                ("IMMEDIATE_OR_CANCEL", 3),
                ("FILL_OR_KILL", 4),
                ("MARKET_ON_OPEN", 5),
            ],
            Some(3),
        )?;
        anyhow::ensure!((1..=5).contains(&time_in_force), "Unknown time in force");
        Ok(Self {
            order_id: positive_id(value, "orderId")?,
            position_id: optional_unsigned(value, "positionId")?
                .map(|id| {
                    anyhow::ensure!(id > 0, "Nonpositive broker position identity");
                    Ok(id)
                })
                .transpose()?,
            symbol_id,
            client_order_id: value
                .get("clientOrderId")
                .filter(|v| !v.is_null())
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .context("Invalid clientOrderId")
                })
                .transpose()?,
            order_type,
            order_status,
            trade_side,
            quantity,
            filled_quantity,
            execution_price: optional_decimal(value, "executionPrice")?,
            limit_price: optional_decimal(value, "limitPrice")?,
            stop_price: optional_decimal(value, "stopPrice")?,
            time_in_force,
            expiration_timestamp_ms: optional_integer(value, "expirationTimestamp")?,
            closing_order: value
                .get("closingOrder")
                .filter(|v| !v.is_null())
                .map(|v| v.as_bool().context("Invalid closingOrder flag"))
                .transpose()?,
            open_timestamp_ms,
            update_timestamp_ms,
        })
    }
}

/// One hedged position; separate broker IDs must never be netted by symbol.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BrokerPosition {
    pub position_id: u64,
    pub symbol_id: u64,
    pub trade_side: u32,
    pub quantity: Decimal,
    pub position_status: u32,
    pub price: Decimal,
    pub open_timestamp_ms: i64,
    pub update_timestamp_ms: i64,
}
impl BrokerPosition {
    pub fn from_wire(value: &Value) -> Result<Self> {
        let position = Self::from_execution_wire(value)?;
        anyhow::ensure!(
            matches!(position.position_status, 1 | 2)
                && (position.position_status != 1 || position.quantity > Decimal::ZERO)
                && position.open_timestamp_ms > 0
                && position.update_timestamp_ms > 0
                && position.price > Decimal::ZERO,
            "Incomplete canonical position evidence"
        );
        Ok(position)
    }

    /// Execution events can carry a position before optional broker clocks/price arrive.
    /// Zero is an internal unknown sentinel, never a published price or timestamp.
    pub fn from_execution_wire(value: &Value) -> Result<Self> {
        let trade = value
            .get("tradeData")
            .context("Missing position tradeData")?;
        let (symbol_id, quantity, trade_side) = trade_identity(trade)?;
        let open_timestamp_ms = optional_integer(trade, "openTimestamp")?.unwrap_or(0);
        let position_status = enum_field(
            value,
            "positionStatus",
            &[
                ("POSITION_STATUS_OPEN", 1),
                ("POSITION_STATUS_CLOSED", 2),
                ("POSITION_STATUS_CREATED", 3),
                ("POSITION_STATUS_ERROR", 4),
            ],
            None,
        )?;
        let price = optional_decimal(value, "price")?;
        anyhow::ensure!(
            price.is_none_or(|p| p >= Decimal::ZERO),
            "Invalid position price"
        );
        let price = price.unwrap_or(Decimal::ZERO);
        let update_timestamp_ms = optional_integer(value, "utcLastUpdateTimestamp")?.unwrap_or(0);
        anyhow::ensure!(
            matches!(position_status, 1 | 2 | 3)
                && open_timestamp_ms >= 0
                && update_timestamp_ms >= 0
                && (open_timestamp_ms == 0
                    || update_timestamp_ms == 0
                    || update_timestamp_ms >= open_timestamp_ms),
            "Invalid position evidence"
        );
        Ok(Self {
            position_id: positive_id(value, "positionId")?,
            symbol_id,
            trade_side,
            quantity,
            position_status,
            price,
            open_timestamp_ms,
            update_timestamp_ms,
        })
    }
}
