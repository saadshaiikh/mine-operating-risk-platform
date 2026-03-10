class IngestionError(Exception):
    """Base exception for ingestion pipeline errors."""


class SourceDownloadError(IngestionError):
    """Raised when a source download fails."""


class DataValidationError(IngestionError):
    """Raised when required data validations fail."""
