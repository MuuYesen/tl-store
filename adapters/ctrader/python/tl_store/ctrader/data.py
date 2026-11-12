"""Open API spot events mapped through the official queued data interface."""

import asyncio
from decimal import Decimal
from nautilus_trader.live.clients import MarketDataClient
from nautilus_trader.model import Quantity, QuoteTick
from .provider import integer, load_contracts
from .reports import price


class CTraderDataClient(MarketDataClient):
    def __init__(self, *, options, session, **kwargs):
        super().__init__(**kwargs)
        self.options, self.session = options, session
        self.transport = None
        self.contracts = {}
        self._subscribed = set()
        self._sides = {}
        self._reader = None
        self._last_source_ts = 0

    async def _connect(self):
        try:
            self.transport = await self.session.acquire(execution=False, market=True)
            self._account_id = self.transport.account_id
            self.contracts = await load_contracts(
                self.transport, self.options, self.clock.timestamp_ns()
            )
            for spec in self.contracts.values():
                self._handle_instrument(spec.instrument)
            self._reader = self.create_task(self._events(), "ctrader-openapi-quotes")
        except BaseException:
            await self._disconnect()
            raise

    async def _disconnect(self):
        self.background_errors = []
        try:
            if self._reader:
                self._reader.cancel()
                outcomes = await asyncio.gather(self._reader, return_exceptions=True)
                self.background_errors = [
                    e
                    for e in outcomes
                    if isinstance(e, BaseException)
                    and not isinstance(e, asyncio.CancelledError)
                ]
        finally:
            self._reader = None
            if self.transport:
                await self.transport.close()
            self.transport = None
            self._sides.clear()
            self._subscribed.clear()
            self._last_source_ts = 0

    def _spec(self, instrument_id):
        matches = [
            s for s in self.contracts.values() if s.instrument.id == instrument_id
        ]
        if len(matches) != 1:
            raise ValueError("Unknown cTrader instrument")
        return matches[0]

    async def _subscribe_quotes(self, command):
        spec = self._spec(command.instrument_id)
        if spec.symbol_id in self._subscribed:
            return
        self._subscribed.add(spec.symbol_id)
        try:
            await self.transport.request(
                "subscribe_spots",
                {"symbolId": [spec.symbol_id], "subscribeToSpotTimestamp": True},
            )
        except BaseException:
            self._subscribed.discard(spec.symbol_id)
            self._sides.pop(spec.symbol_id, None)
            raise

    async def _unsubscribe_quotes(self, command):
        spec = self._spec(command.instrument_id)
        if spec.symbol_id not in self._subscribed:
            return
        await self.transport.request(
            "unsubscribe_spots", {"symbolId": [spec.symbol_id]}
        )
        self._subscribed.remove(spec.symbol_id)
        self._sides.pop(spec.symbol_id, None)

    async def _events(self):
        try:
            await self._events_inner()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            if self.transport is not None:
                await self.transport.halt()
            await self.session.fail(error)
            raise

    async def _events_inner(self):
        while True:
            event = await self.transport.next_event()
            if event["payload_type"] != 2131:
                continue
            payload = event["payload"]
            sid = integer(payload["symbolId"])
            if sid not in self._subscribed:
                continue
            spec = self.contracts[sid]
            received = integer(event["received_wall_ns"])
            if integer(payload.get("ctidTraderAccountId", 0)) != self._account_id:
                raise ValueError("Spot account differs from bound account")
            if "timestamp" not in payload:
                continue
            source = integer(payload["timestamp"])
            if source <= 0 or source < self._last_source_ts:
                continue
            self._last_source_ts = source
            timestamp = source * 1000000
            sides = self._sides.setdefault(sid, {})
            for side in ("bid", "ask"):
                if side in payload:
                    raw = integer(payload[side])
                    if raw <= 0:
                        raise ValueError("Invalid nonpositive spot price")
                    if side not in sides or timestamp >= sides[side][0]:
                        sides[side] = (timestamp, Decimal(raw) / 100000)
            if not all(s in sides for s in ("bid", "ask")):
                continue
            bid, ask = sides["bid"][1], sides["ask"][1]
            if bid > ask:
                raise ValueError("Crossed cTrader quote")
            # Spot protocol carries no size. Zero denotes unavailable quoted size.
            self._handle_data(
                QuoteTick(
                    spec.instrument.id,
                    price(bid, spec),
                    price(ask, spec),
                    Quantity.from_str("0"),
                    Quantity.from_str("0"),
                    timestamp,
                    received,
                )
            )

    async def _subscribe_instruments(self, command):
        for spec in self.contracts.values():
            self._handle_instrument(spec.instrument)

    async def _subscribe_instrument(self, command):
        self._handle_instrument(self._spec(command.instrument_id).instrument)
