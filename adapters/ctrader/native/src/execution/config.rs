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

//! Account-bound economics evidence and execution limits.
/// Raw broker charge sign, verified against broker deal and statement evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommissionSign {
    ChargePositive,
    ChargeNegative,
}

/// Verified interpretation of the broker commission field for this exact account.
/// No default exists: broker sample and statement reconciliation is a prerequisite.
#[derive(Clone, Debug)]
pub struct VerifiedDealEconomics {
    pub ctid_trader_account_id: u64,
    pub trader_login: u64,
    pub deposit_asset_id: u64,
    pub commission_currency: String,
    pub commission_sign: CommissionSign,
    pub evidence_reference: String,
}
