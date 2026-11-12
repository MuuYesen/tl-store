"""Installed candidate has one implementation namespace and the official NA boundary."""
from importlib.metadata import distribution, PackageNotFoundError
from importlib.util import find_spec
import pytest


def test_public_namespace_owns_classes_and_native_without_old_alias():
    from tl_store import ctrader
    from tl_store.ctrader import _native
    assert ctrader.CTraderDataClientFactory.__module__ == 'tl_store.ctrader.factories'
    assert ctrader.CTraderAccountSession.__module__ == 'tl_store.ctrader.session'
    assert _native.__name__ == 'tl_store.ctrader._native'
    assert find_spec('lq_adapter_ctrader') is None
    assert distribution('tl-store').version == '0.2.1a5'
    with pytest.raises(PackageNotFoundError):
        distribution('lq-adapter-ctrader')
    assert not any('trade-learn' in r or 'leafquant' in r for r in distribution('tl-store').requires)
