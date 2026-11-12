"""cTrader Open API adapter for the official NautilusTrader Python runtime."""

from .compat import require_official_nautilus
require_official_nautilus()

from .config import CTraderConfig, CTraderCredentials, VerifiedDealEconomics
from .session import CTraderAccountSession, CTraderAccountObserver
from .factories import CTraderDataClientFactory, CTraderExecutionClientFactory

__all__ = [
    "CTraderConfig", "CTraderCredentials",
    "VerifiedDealEconomics",
    "CTraderAccountSession",
    "CTraderAccountObserver",
    "CTraderDataClientFactory",
    "CTraderExecutionClientFactory",
    "import_oauth_token_state",
]


def import_oauth_token_state(*args, **kwargs):
    """Explicit managed-grant import/CAS; secrets are never logged."""
    from ._native import import_oauth_token_state as native_import

    return native_import(*args, **kwargs)

from .store import CTraderStoreConfig, build_adapter

__all__ += ["CTraderStoreConfig", "build_adapter"]
