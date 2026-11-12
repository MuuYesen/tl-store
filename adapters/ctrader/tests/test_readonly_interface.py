"""Installed native read-only capture seam; never opens a broker account."""

import asyncio
import json

from tl_store.ctrader._native import AccountConnection
from tl_store.ctrader.session import NativeTransport


def test_native_connection_exposes_readonly_state():
    assert callable(getattr(AccountConnection, "state", None))


def test_transport_state_is_native_json_without_python_clock_or_identity_synthesis():
    expected = dict(
        session_id="native-fixture", epoch=3, revision=7, mono_ns=11, utc_ns=13
    )

    class Native:
        async def state(self):
            return json.dumps(expected)

    assert asyncio.run(NativeTransport(Native()).state()) == expected


def test_real_data_factory_client_is_inert_and_exposes_same_readonly_session(
    tmp_path, monkeypatch
):
    from nautilus_trader.common import Cache, Clock
    from nautilus_trader.config import DataClientConfig
    from tl_store.ctrader import CTraderConfig
    from tl_store.ctrader.factories import CTraderDataClientFactory
    from tl_store.ctrader.session import CTraderAccountSession

    monkeypatch.setenv("HOME", str(tmp_path))
    options = CTraderConfig(
        expected_login=12345, symbols=("BTCUSD",), asset_classes=("CRYPTOCURRENCY",)
    )
    factory = CTraderDataClientFactory(options)
    client = factory.create(
        name="CTRADER", config=DataClientConfig(), cache=Cache(), clock=Clock.new_test()
    )
    assert isinstance(client.session, CTraderAccountSession)
    assert client.session is factory.session and factory.session._native is None
    assert not list(tmp_path.iterdir())


def test_closed_transport_state_fails_without_reopening():
    transport = NativeTransport(None)
    with __import__("pytest").raises(RuntimeError, match="closed"):
        asyncio.run(transport.state())
