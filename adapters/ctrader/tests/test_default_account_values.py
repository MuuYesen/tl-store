"""Default TL broker values come from the existing qualified cTrader funds info."""
from types import SimpleNamespace as N
import time
import pytest
from nautilus_trader import model as m
from nautilus_trader.core import UUID4
from tl_store.ctrader.account_values import CTraderAccountValues
from tl_store.ctrader.execution import project_funds


def binding(tmp_path):
    session=N(failure=None)
    return {'account_provider':CTraderAccountValues(session,100001),
        'exec_factories':{'CTRADER':N(session=session)}}


def funds():
    now=time.clock_gettime_ns(time.CLOCK_MONOTONIC) if hasattr(time,'CLOCK_MONOTONIC') else time.monotonic_ns()
    return dict(schema_version=1,account=dict(ctid_trader_account_id=42,trader_login=100001,deposit_asset_id=2),currency='USD',epoch=3,revision=0,observed_utc_ns=time.time_ns(),started_mono_ns=now-1000000,completed_mono_ns=now-500000,clock_scope='local_observation_window',raw=dict(balance='100',net_unrealized_pnl='10',equity='110',used_margin='2',free_margin='108'))


def account(source):
    balance,audit=project_funds(source)
    event=m.AccountState(m.AccountId('CTRADER-100001'),m.AccountType.MARGIN,[balance],[],True,UUID4(),source['observed_utc_ns'],source['observed_utc_ns'],base_currency=m.Currency.from_str(source['currency']),info={'ctrader_funds':audit})
    return m.MarginAccount(event,False)


def test_actual_native_account_info_never_double_adds_pnl(tmp_path):
    a=binding(tmp_path);provider=a['account_provider'];assert callable(provider)
    raw=funds();native=account(raw)
    class Portfolio:
        def equity(self,*args,**kwargs):pytest.fail('raw broker equity already includes PnL')
    result=provider(N(account=lambda identity:native),Portfolio())
    assert result['cash']==108. and result['equity']==110.
    assert result['cash_source']=='ctrader_free_margin'
    assert result['source_observed_utc_ns']==raw['observed_utc_ns']
    assert not any(k in result for k in ['fundvalue','fundshares'])


@pytest.mark.parametrize('change',[
    lambda s:s['account'].update(trader_login=100002),
    lambda s:s['account'].update(ctid_trader_account_id=0),
    lambda s:s.update(epoch=0),
    lambda s:s.update(revision=True),
    lambda s:s.update(clock_scope='foreign_clock'),
    lambda s:s.update(started_mono_ns=s['completed_mono_ns']+1),
    lambda s:s.update(started_mono_ns=s['completed_mono_ns']-3000000000),
    lambda s:s.update(started_mono_ns=s['started_mono_ns']-4000000000,completed_mono_ns=s['completed_mono_ns']-4000000000),
    lambda s:s['raw'].update(equity='NaN'),
    lambda s:s['raw'].update(used_margin='-1'),
    lambda s:s['raw'].update(free_margin='109'),
])
def test_unqualified_funds_cannot_supply_native_strategy_money(tmp_path,change):
    a=binding(tmp_path);provider=a['account_provider'];assert callable(provider)
    raw=funds();change(raw)
    # Preserve malformed source info directly to test the consuming boundary;
    # project_funds is separately tested on producer-side valid inputs.
    balances=[m.AccountBalance(m.Money(110,m.Currency.from_str('USD')),m.Money(2,m.Currency.from_str('USD')),m.Money(108,m.Currency.from_str('USD')))]
    event=m.AccountState(m.AccountId('CTRADER-100001'),m.AccountType.MARGIN,balances,[],True,UUID4(),max(1,raw['observed_utc_ns']),max(1,raw['observed_utc_ns']),base_currency=m.Currency.from_str('USD'),info={'ctrader_funds':raw})
    native=m.MarginAccount(event,False)
    with pytest.raises((ValueError,RuntimeError)):provider(N(account=lambda identity:native),N())


def test_session_failure_is_not_replaced_with_a_recent_cached_balance(tmp_path):
    a=binding(tmp_path);provider=a['account_provider'];assert callable(provider)
    session=a['exec_factories']['CTRADER'].session;session.failure=RuntimeError('synthetic primary')
    with pytest.raises(RuntimeError):provider(N(account=lambda identity:account(funds())),N())


@pytest.mark.parametrize('equity,used,free',[('0','0','0'),('-10','2','-12'),('100','200','-100')])
def test_actual_native_zero_negative_equity_and_available_margin_are_preserved(tmp_path,equity,used,free):
    a=binding(tmp_path);raw=funds();raw['raw'].update(balance=equity,net_unrealized_pnl='0',equity=equity,used_margin=used,free_margin=free)
    result=a['account_provider'](N(account=lambda identity:account(raw)),N())
    assert result['cash']==float(free) and result['equity']==float(equity)
    assert result['source_epoch']==raw['epoch'] and result['source_revision']==raw['revision']

