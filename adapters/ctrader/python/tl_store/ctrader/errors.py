"""Typed nonfatal sampling invalidation; other native failures close admission."""


class FundsRevisionChanged(RuntimeError):
    pass


class FundsObservationChanged(RuntimeError):
    """An account observation cannot qualify; no freshness point may advance."""


class FundsObservationExpired(FundsObservationChanged):
    """Native observation span/age failed while the original epoch remains healthy."""


class CTraderRequestRejected(RuntimeError):
    """Correlated read-only broker rejection; only safe original code is retained."""
    def __init__(self,error_code=None):
        import re
        if error_code is not None and (not isinstance(error_code,str)
                or re.fullmatch(r'[A-Za-z0-9_-]{1,128}',error_code) is None):
            raise ValueError('Invalid protocol error code')
        self.error_code=error_code
        suffix='' if error_code is None else f' [errorCode={error_code}]'
        super().__init__('Open API rejected request'+suffix)
