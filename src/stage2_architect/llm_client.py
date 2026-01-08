"""
OpenRouter API client for calling Claude Sonnet 4.5

Includes LangWatch integration for LLM tracing and observability.
"""

import requests
import json
import logging
import time
from typing import Dict, Any, Optional, Callable
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

logger = logging.getLogger(__name__)

# Retry configuration for transient API failures
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # seconds (exponential backoff: 2s, 4s, 8s)
RETRYABLE_EXCEPTIONS = (
    ChunkedEncodingError,  # Response ended prematurely
    ConnectionError,       # Network connectivity issues
    Timeout,              # Request took too long
)

# LangWatch integration - import with graceful fallback
try:
    from api.services.langwatch_service import (
        update_current_span,
        get_langwatch_service,
        create_llm_span
    )
    _langwatch_available = True
except ImportError:
    _langwatch_available = False
    def update_current_span(*args, **kwargs): pass
    def get_langwatch_service(): return None
    from contextlib import contextmanager
    @contextmanager
    def create_llm_span(name): yield None


class LLMClient:
    """Client for OpenRouter API"""

    def __init__(self, api_key: str, model: str = "anthropic/claude-sonnet-4.5"):
        """
        Initialize LLM client

        Args:
            api_key: OpenRouter API key
            model: Model identifier. Default is Claude Sonnet 4.5.
                   Common alternatives:
                   - "anthropic/claude-sonnet-4.5" (default)
                   - "anthropic/claude-sonnet-4-20250514" (if above doesn't work)
        """
        self.api_key = api_key
        self.endpoint = "https://openrouter.ai/api/v1/chat/completions"
        self.model = model

    def generate_transformer(self, prompt: str) -> str:
        """
        Call LLM to generate transformer code

        Args:
            prompt: Full prompt with CSV sample and instructions

        Returns:
            Generated Python code as string

        Raises:
            Exception if API call fails
        """
        logger.info(f"Calling LLM: {self.model}")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/hvac-etl",
            "X-Title": "HVAC ETL Pipeline"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": 25000,
            "temperature": 0.1  # Low temperature for consistency
        }

        try:
            response = requests.post(
                self.endpoint,
                headers=headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()

            result = response.json()

            if 'choices' not in result or len(result['choices']) == 0:
                raise Exception("Invalid API response format")

            code = result['choices'][0]['message']['content'].strip()

            # Remove markdown code blocks if present
            if code.startswith('```'):
                lines = code.split('\n')
                # Remove first line (```python) and last line (```)
                if len(lines) > 2:
                    # Find the first line after ``` and last line before ```
                    start_idx = 1
                    end_idx = len(lines) - 1
                    # Skip empty lines and language identifier
                    while start_idx < len(lines) and (lines[start_idx].strip() == '' or lines[start_idx].strip().startswith('```')):
                        start_idx += 1
                    while end_idx > 0 and (lines[end_idx].strip() == '' or lines[end_idx].strip().startswith('```')):
                        end_idx -= 1
                    code = '\n'.join(lines[start_idx:end_idx + 1])

            logger.info(f"✅ Received {len(code)} characters of code")

            return code

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise Exception(f"LLM API call failed: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def transform_data(
        self,
        prompt: str,
        max_tokens: int = 16000,
        temperature: float = 0.1,
        job_logger: Optional[Any] = None
    ) -> str:
        """
        Call LLM to transform bronze data to silver format

        Args:
            prompt: Full prompt with transformation instructions and data
            max_tokens: Maximum tokens for response (default 16000 for larger outputs)
            temperature: Temperature for sampling (default 0.1 for consistency)
            job_logger: Optional JobLogger instance for structured logging

        Returns:
            JSON string from LLM (should be valid silver layer JSON)

        Raises:
            Exception if API call fails or response is invalid
        """
        logger.info(f"Calling LLM for data transformation: {self.model}")
        logger.info(f"Prompt size: {len(prompt)} characters ({len(prompt.split())} words)")
        logger.debug(f"Request parameters: max_tokens={max_tokens}, temperature={temperature}, timeout=180s")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/hvac-etl",
            "X-Title": "HVAC ETL Pipeline - Data Transformation"
        }

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "usage": {"include": True}  # Request detailed token usage from OpenRouter
        }

        payload_size = len(json.dumps(payload))
        logger.debug(f"Request payload size: {payload_size:,} bytes ({payload_size/1024:.1f} KB)")

        # Wrap entire LLM call in a LangWatch span for proper tracing
        with create_llm_span("llm_transform") as span:
            try:
                # Update span with input at start
                if span:
                    try:
                        span.update(
                            input=prompt[:2000],  # Truncate for display
                            model=self.model,
                            metadata={"max_tokens": max_tokens, "temperature": temperature}
                        )
                    except Exception as e:
                        logger.debug(f"Failed to update span input: {e}")

                logger.debug(f"Sending POST request to {self.endpoint}")
                start_time = time.time()

                # Retry loop for transient failures
                last_exception = None
                response = None

                for attempt in range(MAX_RETRIES + 1):
                    try:
                        response = requests.post(
                            self.endpoint,
                            headers=headers,
                            json=payload,
                            timeout=180  # Longer timeout for data transformation
                        )
                        response.raise_for_status()
                        break  # Success - exit retry loop

                    except RETRYABLE_EXCEPTIONS as e:
                        last_exception = e
                        if attempt < MAX_RETRIES:
                            delay = RETRY_DELAY_BASE * (2 ** attempt)  # 2s, 4s, 8s
                            logger.warning(f"⚠️ Retryable error on attempt {attempt + 1}/{MAX_RETRIES + 1}: {type(e).__name__}: {e}")
                            logger.warning(f"Retrying in {delay} seconds...")
                            time.sleep(delay)
                        else:
                            logger.error(f"❌ All {MAX_RETRIES + 1} attempts failed")
                            raise Exception(f"LLM API call failed after {MAX_RETRIES + 1} attempts: {str(e)}")

                    except requests.exceptions.HTTPError as e:
                        # Don't retry HTTP errors (4xx, 5xx) - they're not transient
                        logger.error(f"❌ HTTP error occurred: {e}")
                        logger.error(f"Response status: {response.status_code}")
                        logger.error(f"Response body: {response.text[:1000]}")
                        raise Exception(f"LLM API HTTP error ({response.status_code}): {str(e)}")

                elapsed_time = time.time() - start_time
                logger.info(f"API response received in {elapsed_time:.2f} seconds")
                logger.debug(f"Response status code: {response.status_code}")

                result = response.json()
                logger.debug(f"Response JSON parsed successfully")

                if 'choices' not in result or len(result['choices']) == 0:
                    logger.error(f"Invalid API response format: {result}")
                    raise Exception("Invalid API response format")

                content = result['choices'][0]['message']['content'].strip()
                logger.debug(f"Raw content length: {len(content)} characters")

                # Remove markdown code blocks if present
                if content.startswith('```'):
                    logger.debug("Removing markdown code blocks from response")
                    lines = content.split('\n')
                    # Find first line after ``` and last line before ```
                    start_idx = 1
                    end_idx = len(lines) - 1
                    while start_idx < len(lines) and (lines[start_idx].strip() == '' or lines[start_idx].strip().startswith('```') or lines[start_idx].strip() == 'json'):
                        start_idx += 1
                    while end_idx > 0 and (lines[end_idx].strip() == '' or lines[end_idx].strip().startswith('```')):
                        end_idx -= 1
                    content = '\n'.join(lines[start_idx:end_idx + 1])
                    logger.debug(f"Cleaned content length: {len(content)} characters")

                # Verify it's valid JSON
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict) and 'systems' in parsed:
                        logger.debug(f"Validated JSON contains {len(parsed['systems'])} systems")
                except json.JSONDecodeError as e:
                    logger.error(f"LLM returned invalid JSON: {e}")
                    logger.error(f"Content preview (first 500 chars): {content[:500]}")
                    logger.error(f"Content preview (last 500 chars): {content[-500:]}")
                    logger.debug(f"Full invalid content: {content}")
                    raise Exception(f"LLM returned invalid JSON: {str(e)}")

                logger.info(f"✅ Received valid JSON ({len(content)} characters)")

                # Log token usage if available
                estimated_cost = 0.0
                if 'usage' in result:
                    usage = result['usage']
                    prompt_tokens = usage.get('prompt_tokens', 'N/A')
                    completion_tokens = usage.get('completion_tokens', 'N/A')
                    total_tokens = usage.get('total_tokens', 'N/A')
                    logger.info(f"Token usage - Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}")

                    # Calculate cost estimate if tokens are available
                    # Claude Sonnet 4.5 pricing: $3/1M input, $15/1M output tokens
                    if isinstance(prompt_tokens, int) and isinstance(completion_tokens, int):
                        estimated_cost = (prompt_tokens * 0.000003) + (completion_tokens * 0.000015)
                        logger.debug(f"Estimated cost: ${estimated_cost:.4f}")

                # Log rate limit info if available
                if 'x-ratelimit-remaining' in response.headers:
                    logger.debug(f"Rate limit remaining: {response.headers.get('x-ratelimit-remaining')}")

                # Update LangWatch span with output and metrics
                if span and 'usage' in result:
                    usage = result['usage']
                    try:
                        span.update(
                            output=content[:2000],  # Truncate for display
                            metrics={
                                "prompt_tokens": usage.get('prompt_tokens'),
                                "completion_tokens": usage.get('completion_tokens'),
                                "total_tokens": usage.get('total_tokens'),
                                "duration_ms": int(elapsed_time * 1000),
                                "cost": estimated_cost,
                            }
                        )
                    except Exception as e:
                        logger.debug(f"Failed to update span output: {e}")

                # Structured logging via JobLogger if provided
                if job_logger and 'usage' in result:
                    usage = result['usage']
                    langwatch_service = get_langwatch_service()
                    trace_id = langwatch_service.get_current_trace_id() if langwatch_service else None

                    job_logger.llm_call(
                        prompt_preview=prompt,
                        response_preview=content,
                        tokens={
                            "prompt_tokens": usage.get('prompt_tokens', 0),
                            "completion_tokens": usage.get('completion_tokens', 0),
                            "total_tokens": usage.get('total_tokens', 0),
                        },
                        duration_ms=int(elapsed_time * 1000),
                        model=self.model,
                        trace_id=trace_id
                    )

                    # Also record to LineageService for database storage (powers dashboard metrics)
                    if hasattr(job_logger, 'job_id') and job_logger.job_id:
                        try:
                            from api.services.lineage_service import LineageService
                            from api.database.connection import get_db
                            import hashlib

                            with get_db() as db:
                                lineage_service = LineageService(db)
                                lineage_service.record_llm_call(
                                    job_id=job_logger.job_id,
                                    prompt_hash=hashlib.sha256(prompt.encode()).hexdigest()[:16],
                                    prompt_preview=prompt[:500],
                                    response_preview=content[:500],
                                    tokens={
                                        "prompt_tokens": usage.get('prompt_tokens', 0),
                                        "completion_tokens": usage.get('completion_tokens', 0),
                                        "total_tokens": usage.get('total_tokens', 0),
                                    },
                                    duration_ms=int(elapsed_time * 1000),
                                    model=self.model,
                                    trace_id=trace_id
                                )
                        except Exception as e:
                            logger.debug(f"Failed to record LLM call to lineage: {e}")

                return content

            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse API response as JSON: {e}")
                if response:
                    logger.error(f"Response text: {response.text[:1000]}")
                raise Exception(f"Invalid JSON response from API: {str(e)}")
            except Exception as e:
                logger.error(f"❌ Unexpected error during API call: {e}")
                logger.error(f"Error type: {type(e).__name__}", exc_info=True)
                raise
