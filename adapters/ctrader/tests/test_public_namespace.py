"""The installed distribution is TL-Store; cTrader is its first built-in adapter."""
from importlib.metadata import distribution, PackageNotFoundError
import pytest


def test_public_namespace_reuses_existing_classes_without_forwarders():
    from tl_store import ctrader
    import lq_adapter_ctrader as implementation
    assert ctrader.CTraderDataClientFactory is implementation.CTraderDataClientFactory
    assert ctrader.CTraderAccountSession is implementation.CTraderAccountSession
    assert distribution('tl-store').version == '0.2.1a2'
    with pytest.raises(PackageNotFoundError):
        distribution('lq-adapter-ctrader')
    assert not any('trade-learn' in r or 'leafquant' in r for r in distribution('tl-store').requires)
