"""Standard NA views of the one existing native, epoch-bound observed book."""
import json
import asyncio
from decimal import Decimal
from nautilus_trader.model import (BookType,BookOrder,BookAction,OrderSide,Price,Quantity,
                                  OrderBookDelta,OrderBookDeltas,OrderBookDepth,RecordFlag)
from .provider import integer
from .reports import price

BOOK_EVENT_CONTRACT = dict(book_type='L2_MBP',complete=False,source_timestamp_available=False,
                           source_sequence_available=False,order_counts_available=False,
                           price_scale=100000,size_scale=100)
MAX_SYMBOLS=10
MAX_ENTRIES=10000
MAX_LEVELS=1000


class DepthProjection:
    def __init__(self,owner):
        self.owner=owner
        self.books={};self.modes={};self.pending=set();self.stopping=set()
        self.observed=set();self.received={};self.epoch=None
        self._subscription_lock=asyncio.Lock()

    def _command(self,command):
        spec=self.owner._spec(command.instrument_id)
        if getattr(command,'book_type',BookType.L2_MBP)!=BookType.L2_MBP:
            raise ValueError('cTrader observed depth supports L2_MBP only')
        depth=getattr(command,'depth',0)
        if depth is None:depth=0  # Fixed NA SubscribeBookDepth default is optional.
        if type(depth) is not int or not 0<=depth<=MAX_LEVELS:
            raise ValueError('Depth must be an integer from 0 to 1000')
        return spec,depth

    def invalidate(self):
        for book in self.books.values():book.invalidate()
        self.observed.clear();self.received.clear()

    async def fail(self,error):
        self.invalidate()
        if self.owner.transport is not None:await self.owner.transport.halt()
        await self.owner.session.fail(error)

    async def _epoch(self):
        transport=self.owner.transport
        if transport is None:raise RuntimeError('Depth requires the connected read-only market lease')
        current=integer(transport.epoch)
        if current<=0:raise ValueError('Positive native market epoch required')
        if self.epoch is None:self.epoch=current
        elif current!=self.epoch:
            error=ValueError('Depth connection epoch changed; reconnect explicitly')
            await self.fail(error)
            raise error
        return current

    async def subscribe(self,command,mode):
        async with self._subscription_lock:
            return await self._subscribe(command,mode)

    async def _subscribe(self,command,mode):
        spec,depth=self._command(command)
        epoch=await self._epoch();sid=spec.symbol_id
        if sid in self.modes:
            if self.modes[sid].get(mode)==depth:return
            self.modes[sid][mode]=depth
            if sid in self.observed and sid not in self.pending:self.publish(sid,only=mode)
            return
        if len(self.books)>=MAX_SYMBOLS:raise ValueError('Native depth total entry budget exceeded')
        from ._native import DepthBook
        book=DepthBook(integer(self.owner._account_id),sid,epoch,MAX_ENTRIES)
        self.books[sid]=book;self.modes[sid]={mode:depth};self.pending.add(sid)
        try:
            await self.owner.transport.request('subscribe_depth',{'symbolId':[sid]})
        except BaseException:
            book.invalidate();self.books.pop(sid,None);self.modes.pop(sid,None)
            self.observed.discard(sid);self.received.pop(sid,None)
            raise
        finally:self.pending.discard(sid)
        if sid in self.observed:self.publish(sid)

    async def unsubscribe(self,command,mode):
        async with self._subscription_lock:
            return await self._unsubscribe(command,mode)

    async def _unsubscribe(self,command,mode):
        spec,_=self._command(command);sid=spec.symbol_id
        await self._epoch()
        if mode not in self.modes.get(sid,{}):return
        if len(self.modes[sid])>1:
            self.modes[sid].pop(mode);return
        self.stopping.add(sid)
        try:
            await self.owner.transport.request('unsubscribe_depth',{'symbolId':[sid]})
        except BaseException as error:
            await self.fail(error)
            raise
        else:
            self.books.pop(sid).invalidate();self.modes.pop(sid)
            self.observed.discard(sid);self.received.pop(sid,None)
        finally:self.stopping.discard(sid)

    async def event(self,event):
        payload=event['payload'];sid=integer(payload['symbolId'])
        if integer(payload.get('ctidTraderAccountId',0))!=self.owner._account_id:
            raise ValueError('Depth account differs from bound account')
        if sid<=0:raise ValueError('Positive depth symbol required')
        if sid not in self.modes or sid in self.stopping:return
        epoch=await self._epoch()
        received=integer(event['received_wall_ns'])
        if not 0<received<2**64:raise ValueError('Valid native depth receive clock required')
        self.books[sid].apply(json.dumps(payload,separators=(',',':')),epoch)
        self.received[sid]=received;self.observed.add(sid)
        if sid not in self.pending:self.publish(sid)

    def _levels(self,spec,raw,side,depth):
        result=[]
        for p,q in raw[:depth or MAX_LEVELS]:
            px=price(Decimal(p)/100000,spec)
            value=Decimal(q)/100
            size=Quantity.from_str(format(value,f'.{spec.instrument.size_precision}f'))
            if size.as_decimal()!=value:raise ValueError('Depth size loses instrument precision')
            result.append(BookOrder(side,px,size,0))
        return result

    def publish(self,sid,only=None):
        spec=self.owner.contracts[sid]
        view=json.loads(self.books[sid].levels())
        received=self.received[sid]
        mbp,last=RecordFlag.F_MBP.value,RecordFlag.F_LAST.value
        for mode,depth in self.modes[sid].items():
            if only is not None and only!=mode:continue
            bids=self._levels(spec,view['bids'],OrderSide.BUY,depth)
            asks=self._levels(spec,view['asks'],OrderSide.SELL,depth)
            if mode=='depth':
                output=OrderBookDepth(spec.instrument.id,bids,asks,[0]*len(bids),[0]*len(asks),mbp|last,0,0,received)
            else:
                clear=BookOrder(OrderSide.NO_ORDER_SIDE,Price.from_str('0'),Quantity.from_str('0'),0)
                orders=[clear,*bids,*asks]
                deltas=[OrderBookDelta(spec.instrument.id,BookAction.CLEAR if i==0 else BookAction.ADD,
                                      order,mbp|(last if i==len(orders)-1 else 0),0,0,received)
                        for i,order in enumerate(orders)]
                output=OrderBookDeltas(spec.instrument.id,deltas)
            self.owner._handle_data(output)

    async def shutdown(self):
        async with self._subscription_lock:
            return await self._shutdown()

    async def _shutdown(self):
        errors=[]
        try:
            for sid in list(self.modes):
                try:await self.owner.transport.request('unsubscribe_depth',{'symbolId':[sid]})
                except BaseException as error:errors.append(error)
        finally:
            self.invalidate();self.books.clear();self.modes.clear();self.pending.clear()
            self.stopping.clear();self.epoch=None
        return errors
