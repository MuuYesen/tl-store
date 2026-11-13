"""BT facade money from the adapter's existing qualified raw funds observation.

The reported NA balance.total is already broker equity for this adapter. Never
pass it through a generic portfolio equity calculation and add PnL again.
"""
from decimal import Decimal,InvalidOperation
import math
import time


def _monotonic_ns():
    # Match native clock.rs, including macOS CLOCK_MONOTONIC rather than an
    # unrelated local Instant origin. Windows native and Python both use QPC.
    if hasattr(time,'CLOCK_MONOTONIC'):
        return time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    return time.monotonic_ns()


def _integer(value,*,positive=False):
    if type(value) is not int or not (int(positive)<=value<2**64):
        raise ValueError('Invalid cTrader funds identity/clock')
    return value


class CTraderAccountValues:
    def __init__(self,session,login):
        self.session,self.login=session,login

    def __call__(self,cache,portfolio):
        from nautilus_trader.model import AccountId,AccountType,Currency,Money
        if self.session.failure is not None:
            raise RuntimeError('cTrader account processing failed') from self.session.failure
        identity=AccountId(f'CTRADER-{self.login}')
        account=cache.account(identity)
        if account is None or account.id!=identity:
            raise ValueError('Bound cTrader native account unavailable')
        if account.account_type!=AccountType.MARGIN:
            raise ValueError('cTrader margin account source required')
        event=account.last_event
        if event.is_reported is not True:
            raise ValueError('Reported cTrader account observation required')
        info=event.info
        source=info.get('ctrader_funds') if isinstance(info,dict) else None
        if not isinstance(source,dict) or type(source.get('schema_version')) is not int or source.get('schema_version')!=1:
            raise ValueError('Qualified cTrader funds observation required')
        who=source.get('account')
        if not isinstance(who,dict):raise ValueError('cTrader funds account binding required')
        if _integer(who.get('trader_login'),positive=True)!=self.login:
            raise ValueError('cTrader funds login differs from bound account')
        for key in ('ctid_trader_account_id','deposit_asset_id'):
            _integer(who.get(key),positive=True)
        _integer(source.get('epoch'),positive=True)
        _integer(source.get('revision'))
        observed=_integer(source.get('observed_utc_ns'),positive=True)
        if source.get('clock_scope')!='local_observation_window' or event.ts_event!=observed:
            raise ValueError('cTrader funds observation clock differs from account event')
        started=_integer(source.get('started_mono_ns'),positive=True)
        completed=_integer(source.get('completed_mono_ns'),positive=True)
        now=_monotonic_ns()
        if not (started<=completed<=now and completed-started<=2_000_000_000 and now-completed<=3_000_000_000):
            raise ValueError('cTrader funds observation is stale or spans too much time')
        raw=source.get('raw')
        if not isinstance(raw,dict):raise ValueError('Original cTrader funds values required')
        values={}
        for key in ('balance','net_unrealized_pnl','equity','used_margin','free_margin'):
            value=raw.get(key)
            if not isinstance(value,str):raise ValueError('Exact decimal cTrader funds required')
            try:value=Decimal(value)
            except InvalidOperation:raise ValueError('Invalid decimal cTrader funds') from None
            if not value.is_finite():raise ValueError('Finite cTrader funds required')
            values[key]=value
        if (values['used_margin']<0 or values['balance']+values['net_unrealized_pnl']!=values['equity']
                or values['equity']-values['used_margin']!=values['free_margin']):
            raise ValueError('Original cTrader funds do not reconcile')
        code=source.get('currency')
        if not isinstance(code,str) or not code or len(code)>16:
            raise ValueError('cTrader deposit currency required')
        currency=Currency.from_str(code)
        if set(account.balances_total())!={currency}:
            raise ValueError('cTrader funds currency differs from native account')
        expected=Money.from_decimal(values['equity'],currency)
        locked=Money.from_decimal(values['used_margin'],currency)
        if account.balance_total(currency)!=expected or account.balance_locked(currency)!=locked:
            raise ValueError('cTrader funds differ from native account projection')
        cash,equity=float(values['free_margin']),float(values['equity'])
        if not math.isfinite(cash) or not math.isfinite(equity):
            raise ValueError('Finite cTrader broker values required')
        # getcash is the facade's spendable-funds view. Preserve its source
        # meaning; do not relabel it as wallet balance, cash or fund units.
        return dict(cash=cash,equity=equity,currency=code,
            cash_source='ctrader_free_margin',equity_source='ctrader_raw_funds.equity',
            source_observed_utc_ns=observed,source_epoch=source['epoch'],source_revision=source['revision'])
