class LongbridgeError(Exception):
    """Base error for the connector."""


class ConfigurationError(LongbridgeError, ValueError):
    """Raised when connector configuration is invalid."""


class AuthenticationError(LongbridgeError):
    """Raised when credentials are unavailable or rejected."""


class UnsupportedOperation(LongbridgeError):
    """Raised when a venue capability is outside the first phase."""


class SubmissionUncertain(LongbridgeError):
    """Raised when a submit request may have reached the venue but has no proof."""
