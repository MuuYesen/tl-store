"""Fixed NA creates actual book commands and consumes the adapter's standard output."""
import asyncio
from types import MethodType
import pytest
from nautilus_trader.common import Environment,LoggerConfig
from nautilus_trader.config import LiveNodeConfig,DataClientConfig,StrategyConfig
from nautilus_trader.live import LiveNode,SubscribeBookDeltas,SubscribeBookDepth,UnsubscribeBookDepth,UnsubscribeBookDeltas
from nautilus_trader.trading import Strategy
from nautilus_trader.model import TraderId,StrategyId,InstrumentId,BookType,OrderBookDepth,OrderBookDeltas
from tl_store.ctrader import CTraderConfig
from tl_store.ctrader.factories import CTraderDataClientFactory
from tl_store.ctrader.provider import instrument_from_wire


def test_official_node_standard_commands_reach_shared_native_book_and_return_standard_data():
    async def run():
        created=[];commands=[];received=[];requests=[]
        class Transport:
            account_id=42;epoch=3
            def __init__(self):self.queue=asyncio.Queue();self.closed=False
            async def request(self,name,payload):
                requests.append((name,payload))
                if name=='subscribe_depth':
                    await self.queue.put(dict(payload_type=2155,received_wall_ns=999_000_000,payload=dict(ctidTraderAccountId=42,symbolId=1,newQuotes=[dict(id='1',bid='10000000',size='125'),dict(id='2',ask='10001000',size='250')])))
                return {'ctidTraderAccountId':42}
            async def next_event(self):return await self.queue.get()
            async def halt(self):self.closed=True
            async def close(self):self.closed=True
        class Session:
            failure=None
            async def fail(self,error):self.failure=error
        session=Session()
        options=CTraderConfig(expected_login=12345,symbols=('BTCUSD',),asset_classes=('CRYPTOCURRENCY',))
        class Factory(CTraderDataClientFactory):
            def create(self,**kwargs):
                c=super().create(**kwargs);created.append(c)
                original_depth=c._subscribe_book_depth;original_deltas=c._subscribe_book_deltas
                async def depth(self,command):commands.append(command);await original_depth(command)
                async def deltas(self,command):commands.append(command);await original_deltas(command)
                c._subscribe_book_depth=MethodType(depth,c);c._subscribe_book_deltas=MethodType(deltas,c)
                original_unsub_depth=c._unsubscribe_book_depth;original_unsub_deltas=c._unsubscribe_book_deltas
                async def undepth(self,command):commands.append(command);await original_unsub_depth(command)
                async def undeltas(self,command):commands.append(command);await original_unsub_deltas(command)
                c._unsubscribe_book_depth=MethodType(undepth,c);c._unsubscribe_book_deltas=MethodType(undeltas,c)
                async def connect(self):
                    self.transport=Transport();self._account_id=42
                    spec=instrument_from_wire(dict(symbolId=1,symbolName='BTCUSD',baseAssetId=1,quoteAssetId=2),dict(symbolId=1,digits=2,minVolume=1,maxVolume=1000000,stepVolume=1),[dict(assetId=1,name='BTC'),dict(assetId=2,name='USD')],'CRYPTOCURRENCY',1)
                    self.contracts={1:spec};self._handle_instrument(spec.instrument)
                    self._reader=self.create_task(self._events(),'offline-depth-reader')
                c._connect=MethodType(connect,c);return c
        class Receiver(Strategy):
            def on_start(self):
                iid=InstrumentId.from_str('BTCUSD.CTRADER')
                self.subscribe_book_depth(iid,BookType.L2_MBP,depth=10)
                self.subscribe_book_deltas(iid,BookType.L2_MBP)
            def save(self,value):
                received.append(value)
                if len({type(v) for v in received})==2 and not getattr(self,'stop_queued',False):
                    self.stop_queued=True
                    iid=InstrumentId.from_str('BTCUSD.CTRADER')
                    self.unsubscribe_book_depth(iid);self.unsubscribe_book_deltas(iid)
                    async def stop():
                        await asyncio.sleep(.02);self.shutdown_system('offline book output qualified')
                    asyncio.create_task(stop())
            def on_book_depth(self,value):self.save(value)
            def on_book_deltas(self,value):self.save(value)
        node=LiveNode.build('DEPTH-OFFLINE',LiveNodeConfig(environment=Environment.SANDBOX,trader_id=TraderId('DEPTH-001'),logging=LoggerConfig(bypass_logging=True),timeout_connection_secs=2,timeout_disconnection_secs=1,delay_post_stop_secs=0,data_clients={'CTRADER':DataClientConfig()},exec_clients={}),data_factories={'CTRADER':Factory(options,session)})
        receiver=Receiver(StrategyConfig(strategy_id=StrategyId('DEPTH-CHECK-001')))
        node.add_strategy(receiver)
        try:
            async with asyncio.timeout(6):await node.run_async()
            assert {type(c) for c in commands}=={SubscribeBookDepth,SubscribeBookDeltas,UnsubscribeBookDepth,UnsubscribeBookDeltas}
            assert all(c.book_type==BookType.L2_MBP for c in commands if isinstance(c,(SubscribeBookDepth,SubscribeBookDeltas)))
            assert {type(v) for v in received}=={OrderBookDepth,OrderBookDeltas}
            assert all(v.ts_event==0 and v.ts_init==999_000_000 for v in received)
            assert requests==[('subscribe_depth',{'symbolId':[1]}),('unsubscribe_depth',{'symbolId':[1]})]
            assert session.failure is None
        finally:
            node.dispose();receiver=None;created.clear()
    asyncio.run(run())
