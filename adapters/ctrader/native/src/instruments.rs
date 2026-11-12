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

//! Broker-resolved CFD quantities and price increments without MT5 lot assumptions.

use anyhow::{Context, Result, ensure};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::Value;

fn positive_integer(value: &Value, field: &str) -> Result<i64> {
    let raw = value
        .get(field)
        .with_context(|| format!("Missing {field}"))?;
    let number = match raw {
        Value::String(text) => text.parse::<i64>()?,
        Value::Number(number) => number.as_i64().context("Invalid integer wire value")?,
        _ => anyhow::bail!("Invalid integer wire value"),
    };
    ensure!(number > 0, "Nonpositive {field}");
    Ok(number)
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let text = value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("Missing {field}"))?;
    ensure!(
        !text.is_empty()
            && text.len() <= 128
            && text
                .bytes()
                .all(|b| (0x21..=0x7e).contains(&b) && b != b'|'),
        "Invalid broker text field"
    );
    Ok(text)
}

fn asset_name(assets: &Value, identity: i64) -> Result<String> {
    let rows = assets.as_array().context("Missing asset list")?;
    let mut found = None;
    for asset in rows {
        if positive_integer(asset, "assetId")? != identity {
            continue;
        }
        ensure!(found.is_none(), "Ambiguous asset identity");
        found = Some(text(asset, "name")?.to_owned());
    }
    found.context("Broker asset identity not resolved")
}

/// Authoritative symbol metadata; all quantities are underlying units, not lots.
#[derive(Clone, Debug, PartialEq)]
pub struct CfdSymbolSpec {
    pub symbol_id: i64,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub price_increment: Decimal,
    pub minimum_quantity: Decimal,
    pub maximum_quantity: Decimal,
    pub quantity_step: Decimal,
    pub lot_size: Option<Decimal>,
}

impl CfdSymbolSpec {
    /// Joins matching light/full symbols and resolves both currencies by broker asset ID.
    pub fn from_wire(light: &Value, full: &Value, assets: &Value) -> Result<Self> {
        let symbol_id = positive_integer(light, "symbolId")?;
        ensure!(
            symbol_id == positive_integer(full, "symbolId")?,
            "Symbol identity mismatch"
        );
        let digits = full
            .get("digits")
            .and_then(Value::as_u64)
            .context("Missing price digits")?;
        ensure!(digits <= 16, "Unsupported price precision");
        let price_precision = u32::try_from(digits)?;
        let minimum_quantity =
            Decimal::from_i128_with_scale(i128::from(positive_integer(full, "minVolume")?), 2);
        let maximum_quantity =
            Decimal::from_i128_with_scale(i128::from(positive_integer(full, "maxVolume")?), 2);
        let quantity_step =
            Decimal::from_i128_with_scale(i128::from(positive_integer(full, "stepVolume")?), 2);
        ensure!(
            minimum_quantity <= maximum_quantity,
            "Inverted quantity bounds"
        );
        let lot_size = if full.get("lotSize").is_none_or(Value::is_null) {
            None
        } else {
            Some(Decimal::from_i128_with_scale(
                i128::from(positive_integer(full, "lotSize")?),
                2,
            ))
        };
        Ok(Self {
            symbol_id,
            symbol: text(light, "symbolName")?.to_owned(),
            base_asset: asset_name(assets, positive_integer(light, "baseAssetId")?)?,
            quote_asset: asset_name(assets, positive_integer(light, "quoteAssetId")?)?,
            price_precision,
            price_increment: Decimal::from_i128_with_scale(1, price_precision),
            minimum_quantity,
            maximum_quantity,
            quantity_step,
            lot_size,
        })
    }

    /// Validates an exact business quantity and returns the Open API integer centi-unit volume.
    pub fn validate_quantity(&self, quantity: Decimal) -> Result<i64> {
        ensure!(
            quantity > Decimal::ZERO
                && quantity >= self.minimum_quantity
                && quantity <= self.maximum_quantity,
            "Quantity outside broker bounds"
        );
        ensure!(
            quantity.checked_rem(self.quantity_step) == Some(Decimal::ZERO),
            "Quantity is not aligned to broker step"
        );
        let wire = quantity
            .checked_mul(Decimal::from(100))
            .context("Wire volume overflow")?;
        ensure!(
            wire.fract().is_zero(),
            "Wire volume requires integer centi-units"
        );
        wire.to_i64().context("Wire volume exceeds integer bounds")
    }

    /// Rejects invalid prices instead of silently rounding an order to another price.
    pub fn validate_price(&self, price: Decimal) -> Result<()> {
        ensure!(price > Decimal::ZERO, "Nonpositive order price");
        ensure!(
            price.checked_rem(self.price_increment) == Some(Decimal::ZERO),
            "Price is not aligned to broker increment"
        );
        Ok(())
    }
}
