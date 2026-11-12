"""Backtrader-shaped assembly; every broker operation remains offline."""
import os
import pytest
pytest.importorskip('tradelearn')
from tradelearn import engine as bt


def simple(**changes):
    parameters = dict(adapter='ctrader', account=100001, client_id='synthetic-client',
        client_secret='synthetic-secret', access_token='synthetic-access',
        refresh_token='synthetic-refresh', practice=True)
    parameters.update(changes)
    return __import__("tradelearn_runtime", fromlist=["NautilusStore"]).NautilusStore(**parameters)


def test_simple_inert_isolated_and_internal_bar(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    before = dict(os.environ)
    a, b = simple(), simple(account=100002, refresh_token='other-refresh')
    feed = a.getdata(dataname='BTCUSD', timeframe=bt.TimeFrame.Seconds,
                     compression=1, price='mid')
    assert str(feed.bar_type) == 'BTCUSD.CTRADER-1-SECOND-MID-INTERNAL'
    assert a.account_id == 'CTRADER-100001'
    assert a.getbroker() is not b.getbroker()
    da, ea = a.data_factories['CTRADER'], a.exec_factories['CTRADER']
    assert da.session is ea.session
    assert da.session is not b.data_factories['CTRADER'].session
    assert da.options.symbols == ('BTCUSD',)
    assert da.options.credentials.client_secret == 'synthetic-secret'
    assert 'synthetic-secret' not in repr(da.options)
    assert not da.options.live and simple(practice=False).data_factories['CTRADER'].options.live
    assert before == dict(os.environ) and not list(tmp_path.iterdir())


@pytest.mark.parametrize('changes', [dict(account=True),dict(account=0),dict(practice='demo'),
    dict(client_secret=''),dict(unknown=True)])
def test_invalid_connection_parameters(changes):
    with pytest.raises((TypeError, ValueError)):
        simple(**changes)


@pytest.mark.parametrize('parameters', [dict(dataname=''),dict(dataname='BTCUSD',price='last'),
    dict(dataname='BTCUSD',compression=True),dict(dataname='BTCUSD',compression=0),
    dict(dataname='BTCUSD',timeframe=bt.TimeFrame.MicroSeconds,compression=1),
    dict(dataname='BTCUSD',unknown=True)])
def test_feed_rejects_unsupported_requests(parameters):
    with pytest.raises((TypeError, ValueError)):
        simple().getdata(**parameters)


def test_native_and_simple_parameters_cannot_mix():
    with pytest.raises(ValueError, match='conflict|mutually'):
        simple(config=object())
    with pytest.raises(ValueError, match='conflict|mutually'):
        simple().getdata('BTCUSD.CTRADER-1-SECOND-MID-INTERNAL', dataname='BTCUSD')


def test_native_explicit_credentials_do_not_read_global_oauth_or_files(tmp_path, monkeypatch):
    from lq_adapter_ctrader._native import AccountSession
    monkeypatch.setenv('CTRADER_OPENAPI_TOKEN_ROLE', 'invalid-global-value')
    monkeypatch.setenv('CTRADER_OPENAPI_TOKEN_STATE', '/unrelated/private/state')
    # Native construction validates only in-memory bindings, even with hostile globals.
    native = AccountSession(100001, client_id='synthetic-client',
        client_secret='synthetic-secret',access_token='synthetic-access',
        refresh_token='synthetic-refresh', token_state=str(tmp_path/'tokens.json'),
        grant_id='synthetic-grant',token_role='owner')
    assert native is not None and not list(tmp_path.iterdir())


def test_started_store_rejects_feed_before_mutating_options():
    store=simple();store._used=True
    with pytest.raises(RuntimeError, match='after Store start'):
        store.getdata(dataname='BTCUSD')
    assert store.data_factories['CTRADER'].options.symbols == ()


@pytest.mark.parametrize('fail',[False,True])
def test_same_native_lifecycle_releases_adapter_on_exit_and_failure(tmp_path, monkeypatch, fail):
    from types import SimpleNamespace
    monkeypatch.setenv('HOME',str(tmp_path))
    store=simple()
    engine=bt.Cerebro();engine.setbroker(store.getbroker())
    engine.adddata(store.getdata(dataname='BTCUSD'));engine.addstrategy(bt.Strategy)
    calls=[]
    async def close():calls.append('close')
    store._close_adapter=close
    async def drain():calls.append('drain')
    host=SimpleNamespace(frontend=None,drain=drain)
    monkeypatch.setattr(store,'_make_host',lambda engine:host)
    class Node:
        def handle(self):return self
        def add_strategy(self,host):calls.append('add')
        async def run_async(self):
            calls.append('run')
            assert engine._active_store_driver is store
            if fail:raise RuntimeError('offline transport failure')
        def dispose(self):calls.append('dispose')
    monkeypatch.setattr(store,'_build_node',Node)
    if fail:
        with pytest.raises(RuntimeError,match='offline transport failure'):engine.run()
    else:assert engine.run()==[]
    assert calls==['add','run','drain','dispose','close']
    assert engine._active_store_driver is None
    assert store._handle is None


def test_managed_follower_uses_existing_grant_without_seed_or_global_env(tmp_path, monkeypatch):
    monkeypatch.setenv('CTRADER_OPENAPI_TOKEN_ROLE', 'invalid-global-value')
    store=simple(access_token=None,refresh_token=None,token_role='follower',
        token_state=str(tmp_path/'existing.json'),grant_id='existing-managed-grant')
    session=store.data_factories['CTRADER'].session
    assert session.options.credentials.role=='follower'
    assert session.native is not None
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('timeframe,compression,unit,step',[
    (bt.TimeFrame.Weeks,1,'WEEK',1),(bt.TimeFrame.Months,1,'MONTH',1),
    (bt.TimeFrame.Years,1,'YEAR',1),(bt.TimeFrame.MicroSeconds,1000,'MILLISECOND',1),
])
def test_existing_tl_timeframes_map_exactly_to_supported_na_units(timeframe,compression,unit,step):
    feed=simple().getdata(dataname='BTCUSD',timeframe=timeframe,compression=compression)
    assert feed.bar_type.spec.aggregation.name==unit
    assert feed.bar_type.spec.step==step


def test_interval_overflow_rejected_before_native_start():
    with pytest.raises(ValueError):
        simple().getdata(dataname='BTCUSD',timeframe=bt.TimeFrame.Days,compression=2147483647)


def test_simple_entry_preserves_explicit_native_identity_and_admission_parameters():
    from tl_store.ctrader import VerifiedDealEconomics
    evidence=VerifiedDealEconomics(111,100001,1,'USD','charge_positive','synthetic-fixture')
    store=simple(trader_id='OTHER-001',strategy_id='BUSINESS-001',economics=evidence,
        require_flat_on_connect=True,queue_capacity=64,max_history_records=321,
        history_lookback_ms=20000,request_timeout_ms=7000)
    assert str(store.config.trader_id)=='OTHER-001'
    assert str(store.strategy_config.strategy_id)=='BUSINESS-001'
    options=store.data_factories['CTRADER'].options
    assert options.economics is evidence and options.require_flat_on_connect is True
    assert (options.queue_capacity,options.max_history_records,options.history_lookback_ms,
        options.request_timeout_ms)==(64,321,20000,7000)
    assert store.exec_factories['CTRADER'].options is options


@pytest.mark.parametrize('changes',[
    dict(require_flat_on_connect='true'),dict(require_flat_on_connect=1),
    dict(queue_capacity=True),dict(queue_capacity=1.5),dict(max_history_records=True),
    dict(history_lookback_ms=1.5),dict(request_timeout_ms=True),dict(economics=object()),
])
def test_simple_admission_types_fail_in_inert_constructor(changes):
    with pytest.raises((ValueError,TypeError)):
        simple(**changes)


def test_simple_and_native_compile_to_same_account_factories_and_subscription(tmp_path):
    from tl_store.ctrader import CTraderStoreConfig
    simple_store=simple(trader_id='BUSINESS-001',strategy_id='BUSINESS-001')
    simple_data=simple_store.getdata(dataname='BTCUSD')
    options=simple_store.data_factories['CTRADER'].options
    native_store=__import__("tradelearn_runtime", fromlist=["NautilusStore"]).NautilusStore(adapter='ctrader',config=CTraderStoreConfig(
        options=options,node_config=simple_store.config,
        strategy_config=simple_store.strategy_config,state_dir=tmp_path))
    native_data=native_store.getdata(str(simple_data.bar_type))
    assert native_data.bar_type==simple_data.bar_type
    assert native_store.account_id==simple_store.account_id
    assert native_store.intent_identity==simple_store.intent_identity
    for name in ('data_factories','exec_factories'):
        a=getattr(simple_store,name)['CTRADER'];b=getattr(native_store,name)['CTRADER']
        assert type(a) is type(b) and a.options==b.options
        assert a.session is not b.session
    assert native_store.data_factories['CTRADER'].session is native_store.exec_factories['CTRADER'].session
