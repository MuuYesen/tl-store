"""A failed lease cleanup cannot replace the original sampling/admission cause."""
import asyncio
from types import SimpleNamespace as N
import pytest
from tl_store.ctrader.data import CTraderDataClient
from tl_store.ctrader.depth import DepthProjection


@pytest.mark.parametrize('halt_fails,session_fails',[(True,False),(False,True),(True,True)])
def test_event_primary_survives_all_secondary_failures_and_reaches_session(halt_fails,session_fails):
    primary=ValueError('synthetic original');halt_error=RuntimeError('synthetic halt');session_error=RuntimeError('synthetic fail');seen=[]
    async def events():raise primary
    async def halt():
        if halt_fails:raise halt_error
    async def fail(error):
        seen.append(error)
        if session_fails:raise session_error
    owner=N(_events_inner=events,_depth=N(invalidate=lambda:seen.append('invalidated')),transport=N(halt=halt),session=N(fail=fail),background_errors=[])
    with pytest.raises(ValueError) as raised:asyncio.run(CTraderDataClient._events(owner))
    assert raised.value is primary and seen==['invalidated',primary]
    assert owner.background_errors==([halt_error] if halt_fails else [])+([session_error] if session_fails else [])


def test_connect_primary_survives_disconnect_failure(monkeypatch):
    import tl_store.ctrader.data as module
    primary=ValueError('synthetic contracts');secondary=RuntimeError('synthetic cleanup')
    async def acquire(**kwargs):return N(account_id=42)
    async def load(*args):raise primary
    async def disconnect():raise secondary
    owner=N(session=N(acquire=acquire),options=N(),clock=N(timestamp_ns=lambda:1),_disconnect=disconnect,background_errors=[])
    monkeypatch.setattr(module,'load_contracts',load)
    with pytest.raises(ValueError) as raised:asyncio.run(CTraderDataClient._connect(owner))
    assert raised.value is primary and owner.background_errors==[secondary]


def test_failed_depth_unsubscribe_preserves_its_original_and_invalidates_book():
    primary=ValueError('synthetic original');secondary=RuntimeError('synthetic cleanup');seen=[]
    async def request(*args):raise primary
    async def halt():raise secondary
    async def fail(error):seen.append(error)
    owner=N(_spec=lambda iid:N(symbol_id=7),transport=N(epoch=3,request=request,halt=halt),session=N(fail=fail),background_errors=[])
    p=DepthProjection(owner);p.epoch=3;p.modes[7]={'depth':10};p.books[7]=N(invalidate=lambda:seen.append('invalidated'))
    with pytest.raises(ValueError) as raised:asyncio.run(p.unsubscribe(N(instrument_id='synthetic'), 'depth'))
    assert raised.value is primary and seen==['invalidated',primary]
    assert owner.background_errors==[secondary] and not p.observed


def test_close_failure_still_releases_python_transport_and_quote_state():
    secondary=RuntimeError('synthetic close')
    async def close():raise secondary
    async def shutdown():return []
    owner=N(_reader=None,transport=N(close=close),_depth=N(shutdown=shutdown),_sides={1:2},_subscribed={1},_last_source_ts=5)
    with pytest.raises(RuntimeError) as raised:asyncio.run(CTraderDataClient._disconnect(owner))
    assert raised.value is secondary
    assert owner.transport is None and owner._sides=={} and owner._subscribed==set() and owner._last_source_ts==0


def test_disconnect_retains_previously_recorded_cleanup_evidence():
    primary=ValueError('synthetic primary');secondary=RuntimeError('synthetic cleanup')
    async def run():
        async def failed_reader():raise primary
        async def close():pass
        async def shutdown():return []
        reader=asyncio.create_task(failed_reader())
        await asyncio.sleep(0)
        owner=N(_reader=reader,transport=N(close=close),_depth=N(shutdown=shutdown),_sides={},_subscribed=set(),_last_source_ts=0,background_errors=[secondary])
        await CTraderDataClient._disconnect(owner)
        assert owner.background_errors==[secondary,primary]
    asyncio.run(run())
