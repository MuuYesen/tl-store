"""Typed nonfatal sampling invalidation; other native failures close admission."""


class FundsRevisionChanged(RuntimeError):
    pass


class FundsObservationChanged(RuntimeError):
    """An account observation cannot qualify; no freshness point may advance."""


class FundsObservationExpired(FundsObservationChanged):
    """Native observation span/age failed while the original epoch remains healthy."""
