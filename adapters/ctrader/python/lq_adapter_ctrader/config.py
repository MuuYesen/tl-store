"""Account configuration, separate from official node client configs."""

from dataclasses import dataclass, field
from pathlib import Path
from hashlib import sha256


@dataclass(frozen=True)
class VerifiedDealEconomics:
    ctid_trader_account_id: int
    trader_login: int
    deposit_asset_id: int
    commission_currency: str
    commission_sign: str
    evidence_reference: str

    def __post_init__(self):
        if (
            min(self.ctid_trader_account_id, self.trader_login, self.deposit_asset_id)
            <= 0
            or self.commission_sign not in ("charge_positive", "charge_negative")
            or not self.evidence_reference.strip()
        ):
            raise ValueError("Exact account-bound broker fee evidence required")


@dataclass(frozen=True, repr=False)
class CTraderCredentials:
    """Per-instance OAuth seed or existing managed grant; no I/O on construction."""
    client_id: str
    client_secret: str
    access_token: str | None = None
    refresh_token: str | None = None
    state_path: str | Path | None = None
    grant_id: str | None = None
    role: str = 'owner'

    def __post_init__(self):
        for name in ('client_id', 'client_secret', 'access_token', 'refresh_token'):
            if name in ('access_token', 'refresh_token') and getattr(self, name) is None:
                continue
            value = getattr(self, name)
            if (not isinstance(value, str) or not value or len(value) > 16384
                    or not value.isascii() or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
                raise ValueError('Invalid per-instance OAuth credential')
        if self.role not in ('owner', 'follower'):
            raise ValueError('OAuth role must be owner or follower')
        # The same initial grant cannot silently refresh independently for two logins.
        # Existing managed grants can supply their established path and grant_id.
        if (self.access_token is None) != (self.refresh_token is None):
            raise ValueError('Both seed tokens or neither required')
        if self.refresh_token is None and (self.state_path is None or self.grant_id is None):
            raise ValueError('Existing managed state_path and grant_id required without a token seed')
        key = (sha256((self.client_id + '\0' + self.refresh_token).encode()).hexdigest()
               if self.refresh_token is not None else self.grant_id)
        if self.grant_id is None:
            object.__setattr__(self, 'grant_id', key)
        if self.state_path is None:
            object.__setattr__(self, 'state_path', Path.home() / '.tradelearn' / 'oauth' / key / 'tokens.json')
        path = Path(self.state_path)
        if not path.is_absolute() or '..' in path.parts:
            raise ValueError('OAuth state path must be absolute without parent traversal')

    def __repr__(self):
        return 'CTraderCredentials([REDACTED])'


@dataclass(frozen=True)
class CTraderConfig:
    expected_login: int
    symbols: tuple[str, ...]
    asset_classes: tuple[str, ...] = ()
    live: bool = False
    require_flat_on_connect: bool = False
    economics: VerifiedDealEconomics | None = None
    queue_capacity: int = 1024
    max_history_records: int = 100000
    history_lookback_ms: int = 86400000
    request_timeout_ms: int = 10000
    credentials: CTraderCredentials | None = field(default=None, repr=False)
    resolve_asset_classes: bool = False

    def __post_init__(self):
        if isinstance(self.expected_login, bool) or not isinstance(self.expected_login, int) or self.expected_login <= 0:
            raise ValueError("Positive account login required")
        if (
            (not self.symbols and not self.resolve_asset_classes)
            or len(set(self.symbols)) != len(self.symbols)
            or len(self.symbols) > 1000
        ):
            raise ValueError("Explicit unique symbols required")
        if not self.resolve_asset_classes and len(self.asset_classes) != len(self.symbols):
            raise ValueError("One asset class required for each symbol")
        if self.credentials is not None and not isinstance(self.credentials, CTraderCredentials):
            raise TypeError('CTraderCredentials required')
        if any(type(value) is not bool for value in (self.live, self.resolve_asset_classes,
                                                     self.require_flat_on_connect)):
            raise TypeError('Boolean environment, metadata and admission policies required')
        if self.economics is not None and not isinstance(self.economics, VerifiedDealEconomics):
            raise TypeError('VerifiedDealEconomics required')
        if any(type(value) is not int for value in (self.queue_capacity, self.max_history_records,
                                                   self.history_lookback_ms, self.request_timeout_ms)):
            raise TypeError('Integer transport bounds required')
        if self.resolve_asset_classes and self.asset_classes:
            raise ValueError('Explicit and authoritative asset classes are mutually exclusive')
        if (
            not 1 <= self.queue_capacity <= 65536
            or not 1 <= self.max_history_records <= 1000000
            or self.history_lookback_ms <= 0
            or self.request_timeout_ms <= 0
        ):
            raise ValueError("Invalid bounded transport configuration")
        if self.economics and self.economics.trader_login != self.expected_login:
            raise ValueError("Fee evidence belongs to another login")
