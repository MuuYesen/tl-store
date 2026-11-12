//! Account-bound exact financial projection of immutable native history, without network or trading.
use crate::{
    execution::config::{CommissionSign, VerifiedDealEconomics},
    openapi::{OpenApiEnvironment, funds::exact_add, model::amount_from_wire},
};
use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::HashSet;

fn amount(value: &Value, currency: &str) -> Result<Decimal> {
    let raw = value["raw_amount"]
        .as_str()
        .context("Missing raw monetary amount")?
        .parse::<i64>()?;
    let digits = u32::try_from(
        value["money_digits"]
            .as_u64()
            .context("Missing own moneyDigits")?,
    )?;
    let result = amount_from_wire(raw, digits)?;
    ensure!(
        value["amount"]
            .as_str()
            .context("Missing decimal amount")?
            .parse::<Decimal>()?
            == result,
        "Monetary amount and scale disagree"
    );
    ensure!(
        value["currency"].is_null() || value["currency"].as_str() == Some(currency),
        "Monetary settlement currency mismatch"
    );
    if let Some(reason) = value.get("reason").filter(|v| !v.is_null()) {
        ensure!(
            matches!(
                reason["code"].as_str(),
                Some("unverified_currency" | "unverified_fee_economics")
            ),
            "Unsupported source monetary evidence"
        );
    }
    Ok(result)
}
fn charge(raw: Decimal, evidence: &VerifiedDealEconomics) -> Decimal {
    match evidence.commission_sign {
        CommissionSign::ChargePositive => raw,
        CommissionSign::ChargeNegative => -raw,
    }
}
fn money(value: Result<Decimal>, currency: &str) -> Value {
    match value {
        Ok(value) => {
            json!({"amount":value.normalize().to_string(),"currency":currency,"reason":null})
        }
        Err(e) => {
            json!({"amount":null,"currency":currency,"reason":{"code":"incomplete_deal_economics","message":e.to_string()}})
        }
    }
}
fn closing_profit(
    row: &Value,
    currency: &str,
    evidence: &VerifiedDealEconomics,
) -> Result<Decimal> {
    let c = &row["closing"];
    ensure!(c.is_object(), "Missing typed closing detail");
    let closed = c["closed_quantity"]
        .as_str()
        .context("Missing closed volume")?
        .parse::<Decimal>()?;
    let filled = row["filled_quantity"]
        .as_str()
        .context("Missing filled volume")?
        .parse::<Decimal>()?;
    ensure!(
        closed > Decimal::ZERO && closed <= filled,
        "Invalid closed volume"
    );
    let gross = amount(&c["gross_profit"], currency)?;
    let swap = amount(&c["swap"], currency)?;
    // This is the broker's realized allocation for closed volume, including opening commission
    let commission = charge(amount(&c["commission"], currency)?, evidence);
    let conversion = amount(&c["pnl_conversion_fee"], currency)?;
    ensure!(
        conversion >= Decimal::ZERO,
        "Unsupported negative conversion fee"
    );
    exact_add(
        exact_add(exact_add(gross, swap)?, -commission)?,
        -conversion,
    )
}
/// Project one authenticated, normalized deal. Caller must independently bind the
/// event to the trusted account and identity; no history coverage is claimed.
pub fn project_deal_economics(
    row: &mut Value,
    evidence: &VerifiedDealEconomics,
) -> Result<(bool, bool, bool)> {
    let currency = evidence.commission_currency.to_string();
    ensure!(
        !evidence.evidence_reference.trim().is_empty(),
        "Economic evidence required"
    );
    let filled = row["filled_quantity"]
        .as_str()
        .context("Missing deal quantity")?
        .parse::<Decimal>()?;
    if !matches!(row["status"].as_u64(), Some(2 | 3)) || filled <= Decimal::ZERO {
        row["economics"] =
            json!({"version":1,"pnl_status":"not_executed","fee_cost":null,"net_profit":null});
        return Ok((false, false, false));
    }
    let fee = money(
        amount(&row["commission"], &currency).map(|v| charge(v, evidence)),
        &currency,
    );
    let is_closing = !row["closing"].is_null() || !row["close_position_detail"].is_null();
    let net = if is_closing {
        money(closing_profit(row, &currency, evidence), &currency)
    } else {
        json!({"amount":null,"currency":currency,"reason":null})
    };
    let status = if !is_closing {
        "not_applicable"
    } else if net["amount"].is_null() {
        "unavailable"
    } else {
        "complete"
    };
    let missing = fee["amount"].is_null() || status == "unavailable";
    row["economics"] = json!({"version":1,"pnl_status":status,"fee_cost":fee,"net_profit":net});
    Ok((true, is_closing, missing))
}
/// Projects source money into account-bound fee costs and realized net profit.
///
/// The evidence applies to this exact account, environment and deposit currency. Raw fields,
/// source completeness and immutable collection boundaries remain unchanged. This is not a
/// financial ledger or a substitute for independent statement reconciliation.
/// # Errors
/// Returns an error for invalid identity/evidence, incomplete coverage, malformed lists or duplicate IDs.
pub fn project_history_economics(
    mut history: Value,
    evidence: &VerifiedDealEconomics,
    environment: OpenApiEnvironment,
) -> Result<Value> {
    let currency = evidence.commission_currency.to_string();
    let a = &history["account"];
    for (key, expected) in [
        ("ctid_trader_account_id", evidence.ctid_trader_account_id),
        ("trader_login", evidence.trader_login),
        ("deposit_asset_id", evidence.deposit_asset_id),
    ] {
        ensure!(
            expected > 0 && a[key].as_str().and_then(|v| v.parse::<u64>().ok()) == Some(expected),
            "Financial projection account identity mismatch"
        );
    }
    let env = if environment == OpenApiEnvironment::Demo {
        "demo"
    } else {
        "live"
    };
    ensure!(
        a["currency"].as_str() == Some(currency.as_str()) && a["environment"].as_str() == Some(env),
        "Financial projection currency or environment mismatch"
    );
    ensure!(
        !evidence.evidence_reference.trim().is_empty(),
        "Verified economics evidence required"
    );
    let c = &history["coverage"];
    let from = c["from_timestamp_ms"]
        .as_i64()
        .context("Missing history start")?;
    let to = c["to_timestamp_ms"]
        .as_i64()
        .context("Missing history end")?;
    ensure!(
        history["schema_version"] == 1
            && c["complete"] == true
            && c["inclusive"] == true
            && 0 <= from
            && from <= to,
        "Incomplete financial history coverage"
    );
    let rows = history["deals"]
        .as_array_mut()
        .context("Missing history deals")?;
    ensure!(
        rows.len() <= 100000,
        "Financial projection deal bound exceeded"
    );
    let mut ids = HashSet::new();
    let mut missing = 0;
    let mut executed = 0;
    let mut closing = 0;
    for row in rows {
        let id = row["deal_id"]
            .as_str()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|id| *id > 0)
            .context("Invalid financial deal identity")?;
        ensure!(ids.insert(id), "Duplicate financial deal identity");
        let timestamp = row["execution_timestamp_ms"]
            .as_i64()
            .context("Missing deal execution time")?;
        ensure!(
            (from..=to).contains(&timestamp),
            "Financial deal outside history coverage"
        );
        let (is_executed, is_closing, is_missing) = project_deal_economics(row, evidence)?;
        executed += usize::from(is_executed);
        closing += usize::from(is_closing);
        missing += usize::from(is_missing);
    }
    history["financials"] = json!({"version":1,"complete":missing==0,"executed_deals":executed,"closing_deals":closing,"unavailable_deals":missing,"currency":currency,"commission_sign":match evidence.commission_sign {CommissionSign::ChargePositive=>"charge_positive",CommissionSign::ChargeNegative=>"charge_negative"},"evidence_reference":evidence.evidence_reference});
    Ok(history)
}
