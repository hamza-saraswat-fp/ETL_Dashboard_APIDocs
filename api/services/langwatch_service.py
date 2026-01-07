"""
LangWatch Integration Service

Provides helpers for LangWatch tracing and span management.
"""
import logging
from typing import Any, Dict, Optional, Generator
from functools import wraps
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# LangWatch import with graceful fallback
_langwatch_available = False
_langwatch = None

try:
    import langwatch
    _langwatch = langwatch
    _langwatch_available = True
except ImportError:
    logger.warning("LangWatch not installed. Tracing will be disabled.")


class LangWatchService:
    """
    Service for managing LangWatch integration.

    Handles initialization, trace creation, and graceful fallback
    when LangWatch is not configured or available.
    """

    def __init__(self, api_key: Optional[str] = None, enabled: bool = True):
        """
        Initialize LangWatch service.

        Args:
            api_key: LangWatch API key
            enabled: Whether LangWatch is enabled
        """
        self.api_key = api_key
        self.enabled = enabled and _langwatch_available and bool(api_key)
        self._initialized = False

    def initialize(self) -> bool:
        """
        Initialize LangWatch SDK.

        Returns:
            True if initialization successful, False otherwise
        """
        if not self.enabled:
            logger.info("LangWatch disabled or not configured")
            return False

        if self._initialized:
            return True

        try:
            _langwatch.setup(api_key=self.api_key)
            self._initialized = True
            logger.info("LangWatch initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize LangWatch: {e}")
            self.enabled = False
            return False

    def is_available(self) -> bool:
        """Check if LangWatch is available and initialized"""
        return self.enabled and self._initialized

    def get_current_trace(self) -> Optional[Any]:
        """Get the current LangWatch trace if available"""
        if not self.is_available():
            return None
        try:
            return _langwatch.get_current_trace()
        except:
            return None

    def get_current_span(self) -> Optional[Any]:
        """Get the current LangWatch span if available"""
        if not self.is_available():
            return None
        try:
            return _langwatch.get_current_span()
        except:
            return None

    def get_current_trace_id(self) -> Optional[str]:
        """Get the current trace ID if available"""
        trace = self.get_current_trace()
        if trace:
            try:
                return trace.trace_id
            except:
                pass
        return None


# Global service instance (initialized on app startup)
_service: Optional[LangWatchService] = None


def init_langwatch(api_key: str, enabled: bool = True) -> bool:
    """
    Initialize the global LangWatch service.

    Args:
        api_key: LangWatch API key
        enabled: Whether LangWatch is enabled

    Returns:
        True if initialization successful
    """
    global _service
    _service = LangWatchService(api_key, enabled)
    return _service.initialize()


def get_langwatch_service() -> Optional[LangWatchService]:
    """Get the global LangWatch service instance"""
    return _service


def langwatch_trace(name: str, metadata: Optional[Dict[str, Any]] = None):
    """
    Decorator to wrap a function in a LangWatch trace.

    Falls back to no-op if LangWatch is not available.

    Args:
        name: Trace name
        metadata: Optional metadata to attach to the trace
    """
    def decorator(func):
        if not _langwatch_available:
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            service = get_langwatch_service()
            if not service or not service.is_available():
                return func(*args, **kwargs)

            # Use LangWatch trace decorator
            @_langwatch.trace(name=name)
            def traced_func(*args, **kwargs):
                if metadata:
                    trace = _langwatch.get_current_trace()
                    if trace:
                        trace.update(metadata=metadata)
                return func(*args, **kwargs)

            return traced_func(*args, **kwargs)

        return wrapper
    return decorator


def langwatch_span(name: str, type: str = "span"):
    """
    Decorator to wrap a function in a LangWatch span.

    Falls back to no-op if LangWatch is not available.

    Args:
        name: Span name
        type: Span type (e.g., "llm", "retrieval", "tool")
    """
    def decorator(func):
        if not _langwatch_available:
            return func

        @wraps(func)
        def wrapper(*args, **kwargs):
            service = get_langwatch_service()
            if not service or not service.is_available():
                return func(*args, **kwargs)

            # Use LangWatch span decorator
            @_langwatch.span(name=name, type=type)
            def spanned_func(*args, **kwargs):
                return func(*args, **kwargs)

            return spanned_func(*args, **kwargs)

        return wrapper
    return decorator


def update_current_span(
    input: Optional[str] = None,
    output: Optional[str] = None,
    model: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    """
    Update the current LangWatch span with data.

    Args:
        input: Input text (truncated for display)
        output: Output text (truncated for display)
        model: Model identifier
        metrics: Metrics dict (tokens, etc.)
        metadata: Additional metadata
    """
    service = get_langwatch_service()
    if not service or not service.is_available():
        return

    span = service.get_current_span()
    if not span:
        return

    try:
        update_kwargs = {}
        if input is not None:
            update_kwargs["input"] = input[:2000] if len(input) > 2000 else input
        if output is not None:
            update_kwargs["output"] = output[:2000] if len(output) > 2000 else output
        if model is not None:
            update_kwargs["model"] = model
        if metrics is not None:
            update_kwargs["metrics"] = metrics
        if metadata is not None:
            update_kwargs["metadata"] = metadata

        if update_kwargs:
            span.update(**update_kwargs)
    except Exception as e:
        logger.debug(f"Failed to update LangWatch span: {e}")


def update_current_trace(
    metadata: Optional[Dict[str, Any]] = None,
    thread_id: Optional[str] = None
) -> None:
    """
    Update the current LangWatch trace with data.

    Args:
        metadata: Additional metadata
        thread_id: Thread/conversation ID for grouping
    """
    service = get_langwatch_service()
    if not service or not service.is_available():
        return

    trace = service.get_current_trace()
    if not trace:
        return

    try:
        update_kwargs = {}
        if metadata is not None:
            update_kwargs["metadata"] = metadata
        if thread_id is not None:
            update_kwargs["thread_id"] = thread_id

        if update_kwargs:
            trace.update(**update_kwargs)
    except Exception as e:
        logger.debug(f"Failed to update LangWatch trace: {e}")


@contextmanager
def create_llm_span(name: str) -> Generator[Optional[Any], None, None]:
    """
    Context manager for creating LLM spans.

    Creates a span with type="llm" that tracks LLM calls.
    Falls back to yielding None if LangWatch is not available.

    Args:
        name: Span name (e.g., "llm_transform", "llm_extract")

    Yields:
        LangWatch span object if available, None otherwise

    Example:
        with create_llm_span("llm_transform") as span:
            result = call_llm(prompt)
            if span:
                span.update(input=prompt, output=result, model="claude")
    """
    service = get_langwatch_service()
    if not service or not service.is_available() or not _langwatch_available:
        yield None
        return

    try:
        with _langwatch.span(name=name, type="llm") as span:
            yield span
    except Exception as e:
        logger.debug(f"Failed to create LLM span: {e}")
        yield None


@contextmanager
def create_span(name: str, span_type: str = "span") -> Generator[Optional[Any], None, None]:
    """
    Context manager for creating general spans.

    Creates a span with the specified type for tracking pipeline stages.
    Falls back to yielding None if LangWatch is not available.

    Args:
        name: Span name (e.g., "bronze_extraction", "silver_transformation")
        span_type: Span type ("tool", "chain", "span", etc.)

    Yields:
        LangWatch span object if available, None otherwise

    Example:
        with create_span("bronze_extraction", span_type="tool") as span:
            result = extract_data(file)
            if span:
                span.update(output=f"Extracted {len(result)} records")
    """
    service = get_langwatch_service()
    if not service or not service.is_available() or not _langwatch_available:
        yield None
        return

    try:
        with _langwatch.span(name=name, type=span_type) as span:
            yield span
    except Exception as e:
        logger.debug(f"Failed to create span: {e}")
        yield None


def add_span_evaluation(
    span: Optional[Any],
    name: str,
    passed: bool,
    score: Optional[float] = None,
    details: Optional[str] = None
) -> None:
    """
    Add an evaluation result to a LangWatch span.

    Uses span.add_evaluation() to attach quality metrics that appear
    in the LangWatch dashboard for monitoring and alerting.

    Args:
        span: LangWatch span object (can be None for graceful fallback)
        name: Evaluation name (e.g., "completeness", "schema_valid")
        passed: Whether the evaluation passed
        score: Optional score between 0.0 and 1.0
        details: Optional JSON string with evaluation details

    Example:
        with create_span("silver_transformation", span_type="chain") as span:
            # ... transformation logic ...
            add_span_evaluation(
                span,
                name="completeness",
                passed=True,
                score=0.98,
                details='{"missing": 2, "total": 100}'
            )
    """
    if not span:
        return

    service = get_langwatch_service()
    if not service or not service.is_available():
        return

    try:
        eval_kwargs = {
            "name": name,
            "passed": passed
        }
        if score is not None:
            eval_kwargs["score"] = score
        if details is not None:
            eval_kwargs["details"] = details

        span.add_evaluation(**eval_kwargs)
        logger.debug(f"Added evaluation '{name}' to span: passed={passed}, score={score}")
    except Exception as e:
        logger.debug(f"Failed to add evaluation '{name}' to span: {e}")
