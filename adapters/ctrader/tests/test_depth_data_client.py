"""Actual official data-client hooks, shared native book and standard output types."""
import asyncio
from collections import deque
from types import SimpleNamespace as N
import pytest
from nautilus_trader.model import BookType,OrderBookDepth,OrderBookDeltas
from nautilus_trader.common import Cache,Clock
from nautilus_trader.config import DataClientConfig
from tl_store.ctrader import CTraderConfig
from tl_store.ctrader.factories import CTraderDataClientFactory
from tl_store.ctrader.provider import instrument_from_wire


class End(Exception):pass
class Transport:
    account_id=42;epoch=3
    def __init__(self):self.requests=[];self.events=deque();self.halted=False;self.closed=False;self.error=None
    async def request(self,name,payload):
        self.requests.append((name,payload))
        if self.error:raise self.error
        return {'ctidTraderAccountId':42}
    async def next_event(self):
        if not self.events:raise End()
        return self.events.popleft()
    async def halt(self):self.halted=True
    async def close(self):self.closed=True
class Session:
    def __init__(self):self.failure=None
    async def fail(self,error):self.failure=error


def client():
    options=CTraderConfig(expected_login=12345,symbols=('BTCUSD','XAUUSD'),asset_classes=('CRYPTOCURRENCY','METAL'))
    c=CTraderDataClientFactory(options,session=Session()).create(name='CTRADER',config=DataClientConfig(),cache=Cache(),clock=Clock.new_test())
    c.transport=Transport();c._account_id=42
    assets=[dict(assetId=1,name='BTC'),dict(assetId=2,name='USD')]
    for sid,name in [(1,'BTCUSD'),(2,'XAUUSD')]:
        c.contracts[sid]=instrument_from_wire(dict(symbolId=sid,symbolName=name,baseAssetId=1,quoteAssetId=2),dict(symbolId=sid,digits=2,minVolume=1,maxVolume=1000000,stepVolume=1),assets,'CRYPTOCURRENCY',1)
    output=[];c._output=N(data=output.append)
    return c,output


def command(c,sid=1,**kw):return N(**{**dict(instrument_id=c.contracts[sid].instrument.id,book_type=BookType.L2_MBP,depth=10),**kw})
def depth(sid=1,received=999_000_000,**payload):return dict(payload_type=2155,received_wall_ns=received,payload=dict(ctidTraderAccountId=42,symbolId=sid,**payload))
async def drain(c):
    with pytest.raises(End):await c._events_inner()


def test_real_four_hooks_share_one_readonly_subscription_and_preserve_other_mode():
    async def run():
        c,out=client();cmd=command(c)
        await c._subscribe_book_depth(cmd);await c._subscribe_book_deltas(cmd);await c._subscribe_book_depth(cmd)
        assert c.transport.requests==[('subscribe_depth',{'symbolId':[1]})]
        c.transport.events.append(depth(newQuotes=[dict(id='1',bid='10000000',size='125'),dict(id='2',ask='10001000',size='250')]))
        await drain(c)
        assert len(out)==2 and {type(x) for x in out}=={OrderBookDepth,OrderBookDeltas}
        snap=next(x for x in out if isinstance(x,OrderBookDepth))
        assert str(snap.bids[0].price)=='100.00' and str(snap.bids[0].size)=='1.25'
        assert snap.ts_event==0 and snap.ts_init==999_000_000 and snap.sequence==0
        assert snap.bid_counts==[0] and snap.ask_counts==[0]
        await c._unsubscribe_book_depth(cmd)
        assert len(c.transport.requests)==1
        await c._unsubscribe_book_deltas(cmd);await c._unsubscribe_book_deltas(cmd)
        assert c.transport.requests[-1]==('unsubscribe_depth',{'symbolId':[1]})
        assert len(c.transport.requests)==2
    asyncio.run(run())


def test_book_replace_delete_and_multisymbol_do_not_reuse_another_book_or_quote_clock():
    async def run():
        c,out=client()
        for sid in (1,2):await c._subscribe_book_depth(command(c,sid))
        c._last_source_ts=500
        c.transport.events.extend([
            depth(newQuotes=[dict(id='1',bid='10000000',size='100'),dict(id='2',bid='10000000',size='50')]),
            depth(2,newQuotes=[dict(id='1',ask='20000000',size='200')]),
            depth(newQuotes=[dict(id='2',bid='9999000',size='75')],deletedQuotes=['1']),
        ])
        await drain(c)
        assert len(out)==3
        assert str(out[0].bids[0].size)=='1.50'
        assert str(out[1].asks[0].price)=='200.00' and not out[1].bids
        assert str(out[2].bids[0].price)=='99.99' and str(out[2].bids[0].size)=='0.75'
        assert c._last_source_ts==500 and not c._sides
    asyncio.run(run())


@pytest.mark.parametrize('fault',['epoch','foreign','precision','ambiguous'])
def test_standard_depth_failure_poison_epoch_and_release_without_stale_success(fault):
    async def run():
        c,out=client();await c._subscribe_book_depth(command(c))
        event=depth(newQuotes=[dict(id='1',bid='10000000',size='100')])
        if fault=='epoch':c.transport.epoch=4
        elif fault=='foreign':event['payload']['ctidTraderAccountId']=43
        elif fault=='precision':event['payload']['newQuotes'][0]['bid']='10000001'
        else:event['payload']['newQuotes'][0]['ask']='10001000'
        c.transport.events.append(event)
        with pytest.raises((ValueError,RuntimeError)):await c._events()
        assert not out and c.transport.halted and c.session.failure is not None
        lease=c.transport;await c._disconnect();assert lease.closed
    asyncio.run(run())


def test_subscription_failure_rolls_back_and_unsubscription_failure_is_not_success():
    async def run():
        c,out=client();c.transport.error=RuntimeError('readonly rejected')
        with pytest.raises(RuntimeError):await c._subscribe_book_depth(command(c))
        assert not c._depth_modes and not c._depth_books and not out
        c.transport.error=None;await c._subscribe_book_depth(command(c))
        c.transport.error=RuntimeError('unknown unsubscribe result')
        with pytest.raises(RuntimeError):await c._unsubscribe_book_depth(command(c))
        assert c.transport.halted and c.session.failure is not None
    asyncio.run(run())


@pytest.mark.parametrize('bad', [{'depth':True},{'depth':-1},{'depth':1001},{'book_type':BookType.L3_MBO}])
def test_depth_command_validation_precedes_request_and_book_admission(bad):
    async def run():
        c,out=client()
        with pytest.raises(ValueError):await c._subscribe_book_depth(command(c,**bad))
        assert not c.transport.requests and not c._depth_modes and not out
    asyncio.run(run())


def test_optional_depth_bound_native_book_application_and_normal_disconnect():
    async def run():
        from nautilus_trader.model import OrderBook
        c,out=client();await c._subscribe_book_depth(command(c,depth=None));await c._subscribe_book_deltas(command(c,depth=1))
        c.transport.events.extend([
            depth(newQuotes=[dict(id='1',bid='10000000',size='100'),dict(id='2',bid='9999000',size='50'),dict(id='3',ask='10001000',size='75')]),
            depth(newQuotes=[dict(id='2',bid='9998000',size='125')],deletedQuotes=['1']),
        ])
        await drain(c)
        snapshots=[x for x in out if isinstance(x,OrderBookDepth)]
        assert len(snapshots[0].bids)==2 and len(snapshots[1].bids)==1
        native=OrderBook(c.contracts[1].instrument.id,BookType.L2_MBP)
        for batch in (x for x in out if isinstance(x,OrderBookDeltas)):native.apply_deltas(batch)
        assert str(native.best_bid_price())=='99.98'
        lease=c.transport;await c._disconnect()
        assert lease.requests[-1]==('unsubscribe_depth',{'symbolId':[1]}) and lease.closed
        assert not c._depth_books and not c._depth_modes
    asyncio.run(run())


@pytest.mark.parametrize('received',[0,-1,True,2**64])
def test_depth_missing_or_invalid_receive_clock_is_not_a_source_timestamp(received):
    async def run():
        c,out=client();await c._subscribe_book_depth(command(c))
        c.transport.events.append(depth(received=received,newQuotes=[dict(id='1',bid='10000000',size='100')]))
        with pytest.raises(ValueError):await c._events()
        assert not out and c.transport.halted and c.session.failure is not None
    asyncio.run(run())


def test_depth_event_before_ack_is_held_and_concurrent_modes_wait_for_same_ack():
    async def run():
        c,out=client();entered=asyncio.Event();release=asyncio.Event();calls=[]
        async def request(name,payload):
            calls.append((name,payload));entered.set()
            c.transport.events.append(depth(newQuotes=[dict(id='1',bid='10000000',size='100')]))
            await drain(c)
            assert not out
            await release.wait()
            return {'ctidTraderAccountId':42}
        c.transport.request=request
        first=asyncio.create_task(c._subscribe_book_depth(command(c)))
        await entered.wait()
        second=asyncio.create_task(c._subscribe_book_deltas(command(c)))
        await asyncio.sleep(0)
        assert not first.done() and not second.done()
        release.set();await asyncio.gather(first,second)
        assert len(calls)==1
        assert {type(x) for x in out}=={OrderBookDepth,OrderBookDeltas}
        assert all(x.ts_event==0 and x.ts_init==999_000_000 for x in out)
    asyncio.run(run())
