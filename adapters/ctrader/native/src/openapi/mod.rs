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

//! cTrader account/history transport with a private, explicitly invoked startup ClosePosition path.
//! Read-only request and observer interfaces cannot represent trading mutations.

mod client;
mod depth;
mod depth_wire;
pub mod funds;
mod market_stream;
pub mod model;
pub mod oauth;
mod session;
mod symbols;

pub(crate) use client::{
    CancelOrderRequest, MarketOrderRequest, OrderMutationReply, PositionCloseRequest,
};
pub use client::{OpenApiClient, OpenApiEvent, OpenApiEvents};
pub use market_stream::{
    DepthObservation, MarketEvent, MarketObservation, MarketStreamLimits, OpenApiMarketStream,
    RawMarketEvent, SpotObservation,
};
pub use session::{OpenApiAccountSession, OpenApiAdmission, OpenApiLease};

use anyhow::{Result, bail};
use std::time::Duration;

/// cTrader Open API environment. JSON WebSocket traffic uses TLS port 5036.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenApiEnvironment {
    Demo,
    Live,
}

impl OpenApiEnvironment {
    const fn endpoint(self) -> &'static str {
        match self {
            Self::Demo => "wss://demo.ctraderapi.com:5036",
            Self::Live => "wss://live.ctraderapi.com:5036",
        }
    }

    const fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}

/// Transport bounds and exact external login identity, without credentials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenApiConfig {
    pub environment: OpenApiEnvironment,
    pub expected_trader_login: u64,
    pub request_timeout: Duration,
    pub event_capacity: usize,
    pub max_unsolicited_event_bytes: usize,
    pub max_in_flight: usize,
}

impl OpenApiConfig {
    pub fn demo(expected_trader_login: u64) -> Self {
        Self {
            environment: OpenApiEnvironment::Demo,
            expected_trader_login,
            request_timeout: Duration::from_secs(10),
            event_capacity: 1024,
            max_unsolicited_event_bytes: 8 * 1_048_576,
            max_in_flight: 64,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.expected_trader_login == 0
            || self.event_capacity == 0
            || !(1024..=client::MAX_MESSAGE_BYTES).contains(&self.max_unsolicited_event_bytes)
            || self.max_in_flight == 0
            || self.request_timeout.is_zero()
        {
            bail!("invalid Open API transport configuration");
        }
        Ok(())
    }
}

/// OAuth application credentials and one account access token; Debug always redacts secrets.
pub struct OpenApiCredentials {
    client_id: String,
    client_secret: String,
    access_token: String,
}

impl std::fmt::Debug for OpenApiCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OpenApiCredentials([REDACTED])")
    }
}

impl OpenApiCredentials {
    pub fn from_env() -> Result<Self> {
        fn required(name: &str) -> Result<String> {
            match std::env::var(name) {
                Ok(value) if !value.is_empty() => Ok(value),
                _ => bail!("missing Open API credential: {name}"),
            }
        }
        Ok(Self {
            client_id: required("CTRADER_OPENAPI_CLIENT_ID")?,
            client_secret: required("CTRADER_OPENAPI_CLIENT_SECRET")?,
            access_token: required("CTRADER_OPENAPI_ACCESS_TOKEN")?,
        })
    }
}

/// Explicit read-only request set; trading payload types are not representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadOnlyRequest {
    Trendbars,
    Trader,
    Reconcile,
    Deals,
    Orders,
    PositionPnL,
    CashFlows,
    Symbols,
    Assets,
    AssetClasses,
    SymbolCategories,
    SymbolDetails,
    DynamicLeverage,
    SymbolsForConversion,
    SubscribeSpots,
    UnsubscribeSpots,
    SubscribeDepthQuotes,
    UnsubscribeDepthQuotes,
    DealsByPosition,
    OrderDetails,
    OrdersByPosition,
}

impl ReadOnlyRequest {
    /// The existing typed read-only protocol, shared by Python and native callers.
    pub fn named(name: &str) -> Option<Self> {
        Some(match name {
            "trader" => ReadOnlyRequest::Trader,
            "orders" => ReadOnlyRequest::Orders,
            "deals" => ReadOnlyRequest::Deals,
            "reconcile" => ReadOnlyRequest::Reconcile,
            "assets" => ReadOnlyRequest::Assets,
            "asset_classes" => ReadOnlyRequest::AssetClasses,
            "symbol_categories" => ReadOnlyRequest::SymbolCategories,
            "symbols" => ReadOnlyRequest::Symbols,
            "symbol_details" => ReadOnlyRequest::SymbolDetails,
            "position_pnl" => ReadOnlyRequest::PositionPnL,
            "order_details" => ReadOnlyRequest::OrderDetails,
            "deals_by_position" => ReadOnlyRequest::DealsByPosition,
            "orders_by_position" => ReadOnlyRequest::OrdersByPosition,
            "subscribe_spots" => ReadOnlyRequest::SubscribeSpots,
            "unsubscribe_spots" => ReadOnlyRequest::UnsubscribeSpots,
            "subscribe_depth" => ReadOnlyRequest::SubscribeDepthQuotes,
            "unsubscribe_depth" => ReadOnlyRequest::UnsubscribeDepthQuotes,
            "trendbars" => ReadOnlyRequest::Trendbars,
            "cash_flows" => ReadOnlyRequest::CashFlows,
            "dynamic_leverage" => ReadOnlyRequest::DynamicLeverage,
            "symbols_for_conversion" => ReadOnlyRequest::SymbolsForConversion,
            _ => return None,
        })
    }

    const fn request_type(self) -> u32 {
        match self {
            Self::Trendbars => 2137,
            Self::Trader => 2121,
            Self::Reconcile => 2124,
            Self::Deals => 2133,
            Self::Orders => 2175,
            Self::PositionPnL => 2187,
            Self::CashFlows => 2143,
            Self::Symbols => 2114,
            Self::Assets => 2112,
            Self::AssetClasses => 2153,
            Self::SymbolCategories => 2160,
            Self::SymbolDetails => 2116,
            Self::DynamicLeverage => 2177,
            Self::SymbolsForConversion => 2118,
            Self::SubscribeSpots => 2127,
            Self::UnsubscribeSpots => 2129,
            Self::SubscribeDepthQuotes => 2156,
            Self::UnsubscribeDepthQuotes => 2158,
            Self::DealsByPosition => 2179,
            Self::OrderDetails => 2181,
            Self::OrdersByPosition => 2183,
        }
    }

    pub const fn response_type(self) -> u32 {
        match self {
            Self::Trendbars => 2138,
            Self::Trader => 2122,
            Self::Reconcile => 2125,
            Self::Deals => 2134,
            Self::Orders => 2176,
            Self::PositionPnL => 2188,
            Self::CashFlows => 2144,
            Self::Symbols => 2115,
            Self::Assets => 2113,
            Self::AssetClasses => 2154,
            Self::SymbolCategories => 2161,
            Self::SymbolDetails => 2117,
            Self::DynamicLeverage => 2178,
            Self::SymbolsForConversion => 2119,
            Self::SubscribeSpots => 2128,
            Self::UnsubscribeSpots => 2130,
            Self::SubscribeDepthQuotes => 2157,
            Self::UnsubscribeDepthQuotes => 2159,
            Self::DealsByPosition => 2180,
            Self::OrderDetails => 2182,
            Self::OrdersByPosition => 2184,
        }
    }

    const fn is_historical(self) -> bool {
        matches!(
            self,
            Self::Trendbars
                | Self::Deals
                | Self::Orders
                | Self::CashFlows
                | Self::DealsByPosition
                | Self::OrderDetails
                | Self::OrdersByPosition
        )
    }
}

#[cfg(test)]
pub(crate) mod tests;
