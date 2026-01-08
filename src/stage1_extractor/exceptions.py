"""
Custom exceptions for Excel Extractor
"""


class ExtractionError(Exception):
    """Base exception for extraction errors"""
    pass


class FileNotFoundError(ExtractionError):
    """Raised when Excel file is not found"""
    pass


class InvalidFileFormatError(ExtractionError):
    """Raised when file is not a valid Excel format"""
    pass


class SheetProcessingError(ExtractionError):
    """Raised when a sheet cannot be processed"""
    pass


class NoValidSheetsError(ExtractionError):
    """Raised when no valid data sheets are found after filtering"""
    pass


class PDFProcessingError(ExtractionError):
    """Raised when a PDF cannot be processed"""
    pass


class InvalidPDFFormatError(ExtractionError):
    """Raised when file is not a valid PDF format"""
    pass
