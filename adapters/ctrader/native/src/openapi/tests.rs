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

use rust_decimal::Decimal;
use serde_json::json;

use super::model::{BrokerAccountSnapshot, BrokerDeal, amount_from_wire, volume_from_wire};
use super::{OpenApiConfig, ReadOnlyRequest};

pub(crate) fn mock_credentials() -> super::OpenApiCredentials {
    super::OpenApiCredentials {
        client_id: "mock-id".into(),
        client_secret: "mock-secret".into(),
        access_token: "mock-token".into(),
    }
}

#[rstest::rstest]
fn money_and_volume_use_exact_decimal_scaling() {
    assert_eq!(
        amount_from_wire(10_053_099_944, 8).unwrap(),
        Decimal::new(10_053_099_944, 8)
    );
    assert_eq!(volume_from_wire(1_001), Decimal::new(1_001, 2));
    assert!(amount_from_wire(1, 29).is_err());
}

#[rstest::rstest]
fn missing_commission_is_unknown_not_zero() {
    let value = json!({"dealId": "10", "orderId": "20", "positionId": "30",
        "volume": "1001", "filledVolume": "1001", "symbolId": "5",
        "createTimestamp": "100", "executionTimestamp": "108",
        "tradeSide": 1, "dealStatus": 2});
    let deal = BrokerDeal::from_wire(&value).unwrap();
    assert_eq!(deal.filled_volume, Decimal::new(1001, 2));
    assert_eq!(deal.commission, None);
    assert_eq!(deal.execution_price, None);
    assert_eq!(deal.money_digits, None);
    assert_eq!(deal.broker_execution_latency_ms(), Some(8));
}

#[rstest::rstest]
fn execution_price_is_exact_and_missing_price_is_not_inferred() {
    let mut value = json!({"dealId": "10", "orderId": "20", "positionId": "30",
        "volume": "100", "filledVolume": "100", "symbolId": "5",
        "createTimestamp": "100", "executionTimestamp": "108",
        "tradeSide": 1, "dealStatus": 2, "executionPrice": "4123.123456789"});
    let deal = BrokerDeal::from_wire(&value).unwrap();
    assert_eq!(
        deal.execution_price,
        Some(Decimal::new(4_123_123_456_789, 9))
    );
    value["executionPrice"] = json!(true);
    assert!(BrokerDeal::from_wire(&value).is_err());
}

#[rstest::rstest]
fn real_commission_preserves_sign_and_scale() {
    let value = json!({"dealId": "10", "orderId": "20", "positionId": "30",
        "volume": "100", "filledVolume": "100", "symbolId": "5",
        "createTimestamp": "100", "executionTimestamp": "108",
        "tradeSide": 1, "dealStatus": 2, "commission": "-12345678", "moneyDigits": 8});
    let deal = BrokerDeal::from_wire(&value).unwrap();
    assert_eq!(deal.commission, Some(Decimal::new(-12_345_678, 8)));
    assert_eq!(deal.money_digits, Some(8));
}

#[rstest::rstest]
fn account_missing_balance_does_not_default_to_zero() {
    assert!(BrokerAccountSnapshot::from_trader(&json!({"moneyDigits": 2})).is_err());
    let snapshot = BrokerAccountSnapshot::from_trader(&json!({
        "ctidTraderAccountId": "42", "traderLogin": "100001",
        "balance": "10053", "moneyDigits": 2, "depositAssetId": "1"}))
    .unwrap();
    assert_eq!(snapshot.balance, Decimal::new(10053, 2));
    assert_eq!(snapshot.equity, None);
}

#[test]
fn transport_bounds_remain_valid_without_private_rate_settings() {
    let mut config=OpenApiConfig::demo(100_001);
    assert!(config.validate().is_ok());
    config.max_in_flight=0;
    assert!(config.validate().is_err());
}

#[rstest::rstest]
#[case(ReadOnlyRequest::AssetClasses, 2153, 2154, false)]
#[case(ReadOnlyRequest::SymbolCategories, 2160, 2161, false)]
#[case(ReadOnlyRequest::SymbolDetails, 2116, 2117, false)]
#[case(ReadOnlyRequest::DynamicLeverage, 2177, 2178, false)]
#[case(ReadOnlyRequest::SymbolsForConversion, 2118, 2119, false)]
#[case(ReadOnlyRequest::SubscribeDepthQuotes, 2156, 2157, false)]
#[case(ReadOnlyRequest::UnsubscribeDepthQuotes, 2158, 2159, false)]
#[case(ReadOnlyRequest::DealsByPosition, 2179, 2180, true)]
#[case(ReadOnlyRequest::OrderDetails, 2181, 2182, true)]
#[case(ReadOnlyRequest::OrdersByPosition, 2183, 2184, true)]
fn metadata_and_reconciliation_requests_use_official_read_only_types(
    #[case] operation: ReadOnlyRequest,
    #[case] request: u32,
    #[case] response: u32,
    #[case] historical: bool,
) {
    assert_eq!(operation.request_type(), request);
    assert_eq!(operation.response_type(), response);
    assert_eq!(operation.is_historical(), historical);
}
