"""Generic public binding: no account connection or application profile."""
from dataclasses import replace
from pathlib import Path
import pytest
from nautilus_trader.live import LiveNodeConfig
from nautilus_trader.config import DataClientConfig, ExecutionClientConfig
from nautilus_trader.model import TraderId
from tl_store.ctrader import CTraderConfig
from tl_store.ctrader.store import CTraderStoreConfig, build_adapter
from tradelearn_runtime.store import NautilusStore

def assembled(config):
    return NautilusStore(adapter="ctrader",config=config)

def config(path, login=100001):
    return CTraderStoreConfig(options=CTraderConfig(expected_login=login,
        symbols=('BTCUSD',),asset_classes=('CRYPTOCURRENCY',)),
        node_config=LiveNodeConfig(trader_id=TraderId('CLIENT-001'),
            data_clients={'CTRADER':DataClientConfig()},exec_clients={'CTRADER':ExecutionClientConfig()}),
        state_dir=path)

def test_constructs_isolated_without_acquire_or_state_writes(tmp_path, monkeypatch):
    from tl_store.ctrader.session import CTraderAccountSession
    async def forbidden(*args, **kwargs):raise AssertionError('connected during assembly')
    monkeypatch.setattr(CTraderAccountSession,'acquire',forbidden)
    a,b=assembled(config(tmp_path)),assembled(config(tmp_path,100002))
    assert a.account_id=='CTRADER-100001' and b.account_id=='CTRADER-100002'
    assert a.state_path!=b.state_path and a.getbroker() is not b.getbroker()
    da=a.data_factories['CTRADER'];ea=a.exec_factories['CTRADER']
    assert da.session is ea.session
    assert da.session is not b.data_factories['CTRADER'].session
    assert not list(tmp_path.iterdir())
    feed=a.getdata('BTCUSD.CTRADER-1-SECOND-MID-INTERNAL')
    assert feed.bar_type.is_internally_aggregated()
    with pytest.raises(RuntimeError,match='not started'):a.getbroker().getcash()

@pytest.mark.parametrize('bar',['BTCUSD.CTRADER-1-SECOND-MID-EXTERNAL',
    'BTCUSD.CTRADER-1-SECOND-LAST-INTERNAL','OTHER.CTRADER-1-SECOND-MID-INTERNAL',
    'BTCUSD.OTHER-1-SECOND-MID-INTERNAL'])
def test_refuses_unsupported_feed_before_start(tmp_path,bar):
    a=assembled(config(tmp_path))
    with pytest.raises(ValueError):a.getdata(bar)

def test_rejects_wrong_config_and_unknown_parameters(tmp_path):
    with pytest.raises(TypeError):build_adapter({})
    with pytest.raises(TypeError):CTraderStoreConfig(options=None,node_config=None,unknown=True)
    with pytest.raises(ValueError,match='CTRADER'):
        replace(config(tmp_path),node_config=LiveNodeConfig(trader_id=TraderId('CLIENT-001')))

def test_real_official_node_assembles_installed_factories_offline(tmp_path,monkeypatch):
    from tl_store.ctrader.session import CTraderAccountSession
    async def forbidden(*args,**kwargs):raise AssertionError('broker access')
    monkeypatch.setattr(CTraderAccountSession,'acquire',forbidden)
    store=assembled(config(tmp_path));node=store._build_node()
    try:
        assert store._host is None
    finally:node.dispose()


def test_installed_entrypoint_runs_exactly_one_shared_driver(tmp_path, monkeypatch):
    from importlib.metadata import entry_points
    from tradelearn import engine as bt
    from tradelearn_runtime._adapters import _factories
    monkeypatch.delitem(_factories, 'ctrader', raising=False)
    assert len(list(entry_points(group='tradelearn.store_adapters', name='ctrader'))) == 1
    store=__import__("tradelearn_runtime", fromlist=["NautilusStore"]).NautilusStore(adapter='ctrader', config=config(tmp_path))
    engine=bt.Cerebro();engine.setbroker(store.getbroker())
    engine.adddata(store.getdata('BTCUSD.CTRADER-1-SECOND-MID-INTERNAL'))
    engine.addstrategy(bt.Strategy)
    calls=[]
    async def run(current):
        calls.append(current)
        assert current._active_store_driver is store
        raise RuntimeError('offline startup failure')
    monkeypatch.setattr(store, '_run_native', run)
    with pytest.raises(RuntimeError, match='offline startup failure'):engine.run()
    assert calls == [engine] and engine._active_store_driver is None
    assert [event[0] for event in store.get_notifications()] == ['STARTING', 'FAILED']
    with pytest.raises(RuntimeError, match='single use'):engine.run()


def test_default_account_values_selection_is_inert_and_consumed_by_native_broker(tmp_path):
    import time
    from types import SimpleNamespace as N
    from nautilus_trader import model as m
    from nautilus_trader.core import UUID4
    from tradelearn_runtime.broker import NautilusBroker
    from tl_store.ctrader.account_values import CTraderAccountValues
    from tl_store.ctrader.execution import project_funds
    binding=build_adapter(config(tmp_path))
    provider=binding['account_provider']
    assert isinstance(provider,CTraderAccountValues)
    assert binding['exec_factories']['CTRADER'].session._native is None
    now=time.clock_gettime_ns(time.CLOCK_MONOTONIC) if hasattr(time,'CLOCK_MONOTONIC') else time.monotonic_ns()
    source=dict(schema_version=1,account=dict(ctid_trader_account_id=42,trader_login=100001,deposit_asset_id=2),currency='USD',epoch=3,revision=0,observed_utc_ns=time.time_ns(),started_mono_ns=now-1000000,completed_mono_ns=now-500000,clock_scope='local_observation_window',raw=dict(balance='100',net_unrealized_pnl='10',equity='110',used_margin='2',free_margin='108'))
    balance,audit=project_funds(source)
    event=m.AccountState(m.AccountId('CTRADER-100001'),m.AccountType.MARGIN,[balance],[],True,UUID4(),source['observed_utc_ns'],source['observed_utc_ns'],base_currency=m.Currency.from_str('USD'),info={'ctrader_funds':audit})
    native=m.MarginAccount(event,False)
    broker=NautilusBroker(N(account=lambda:provider(N(account=lambda identity:native),N())))
    assert broker.getcash()==108. and broker.getvalue()==110.
    custom=lambda *args:dict(cash=1,equity=2)
    assert build_adapter(replace(config(tmp_path),account_provider=custom))['account_provider'] is custom
