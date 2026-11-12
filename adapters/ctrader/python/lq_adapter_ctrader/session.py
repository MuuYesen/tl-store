"""Explicit account session; consumers own leases and never reconnect invisibly."""

import json
from .errors import FundsRevisionChanged, FundsObservationChanged, FundsObservationExpired


class NativeTransport:
    def __init__(self, connection):
        self.connection = connection

    @property
    def account_id(self):
        return self.connection.account_id

    @property
    def epoch(self):
        return self.connection.epoch

    async def request(self, name, payload):
        return json.loads(
            await self.connection.request(
                name, json.dumps(payload, separators=(",", ":"))
            )
        )

    async def snapshot(self, lookback_ms=86400000, require_flat=False):
        return json.loads(await self.connection.snapshot(lookback_ms, require_flat))

    async def observe(self):
        return json.loads(await self.connection.observe())

    async def check_epoch(self, epoch, revision=None):
        return await self.connection.check_epoch(epoch, revision)

    async def funds(self):
        epoch = self.epoch
        try:
            value = json.loads(await self.connection.funds())
        except RuntimeError as error:
            if str(error) != "Funds observation is stale or spans too much time":
                raise
            # This exact timing check precedes publication of any funds value.
            # A dead/invalid epoch remains fatal; never manufacture a fresh sample.
            await self.connection.check_epoch(epoch)
            raise FundsObservationExpired("Native funds sample exceeded its timing window") from None
        if value == {"status": "revision_changed"}:
            raise FundsRevisionChanged("Reader revision changed; discard this sample")
        if value == {"status": "observation_changed"}:
            raise FundsObservationChanged("Account views changed; discard this sample")
        return value

    async def halt(self):
        self.connection.halt()

    async def next_event(self):
        return json.loads(await self.connection.next_event())

    async def mutate(self, command):
        value = await self.connection.mutate(json.dumps(command, separators=(",", ":")))
        return json.loads(value)

    async def acknowledge_executions(self, through):
        self.connection.acknowledge_executions(through)

    async def pending(self):
        return json.loads(await self.connection.pending())

    async def close(self):
        # Releasing this lease leaves the other explicitly shared consumers alive.
        self.connection = None


class CTraderAccountSession:
    def __init__(self, options):
        self.options = options
        self.failure = None
        self._native = None

    @property
    def native(self):
        if self._native is None:
            from ._native import AccountSession
            options = self.options
            credentials = options.credentials
            keywords = {} if credentials is None else dict(
                client_id=credentials.client_id, client_secret=credentials.client_secret,
                access_token=credentials.access_token, refresh_token=credentials.refresh_token,
                token_state=str(credentials.state_path), grant_id=credentials.grant_id,
                token_role=credentials.role)
            self._native = AccountSession(
                options.expected_login,
                options.live,
                options.request_timeout_ms,
                options.queue_capacity,
                **keywords,
            )
        return self._native

    async def acquire(self, execution=False, market=False):
        options = self.options
        if self.failure is not None:
            raise RuntimeError(
                "Account processing epoch failed; recreate session"
            ) from self.failure
        connection = await self.native.acquire(
            execution=execution,
            max_history_records=options.max_history_records,
            isolated=market,
        )
        return NativeTransport(connection)

    async def close(self):
        """Release only this instance's lazy native scope, including OAuth ownership."""
        native, self._native = self._native, None
        if native is not None:
            await native.invalidate()

    async def fail(self, error):
        if self.failure is None:
            # Publish the primary cause before any await can close the reader.
            self.failure = error
            await self.native.invalidate()


class CTraderAccountObserver:
    """One retained read-only lease; epoch health checks never reconnect."""

    def __init__(self, session):
        self.session = session
        self.transport = None
        self._reader = None

    async def connect(self):
        import asyncio

        if self.transport is not None:
            raise RuntimeError("Observer already connected")
        self.transport = await self.session.acquire(execution=False)
        self._reader = asyncio.create_task(self._drain())

    async def _drain(self):
        while True:
            await self.transport.next_event()

    async def snapshot(self):
        if self.transport is None:
            raise RuntimeError("Observer is not connected")
        if self._reader.done():
            await self._reader
        return json.dumps(await self.transport.observe(), separators=(",", ":"))

    async def check_epoch(self, epoch, revision=None):
        if self.session.failure is not None:
            raise RuntimeError(
                "Account processing epoch failed"
            ) from self.session.failure
        if self.transport is None:
            raise RuntimeError("Observer is not connected")
        if self._reader.done():
            await self._reader
        await self.transport.check_epoch(epoch, revision)

    async def disconnect(self):
        import asyncio

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
