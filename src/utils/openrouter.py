"""
DealHunter — OpenRouter API Utilities
Shared constants, headers builder, call wrapper, and JSON parse helper.

Used by: title_generator, product_image_selector.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import structlog

from src.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


def openrouter_headers(api_key: str | None = None) -> dict[str, str]:
    """Build standard headers for OpenRouter API calls."""
    key = api_key or settings.openrouter.api_key
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
        "HTTP-Referer": "https://dealhunter.ai",
        "X-Title": "DealHunter",
    }


# ---------------------------------------------------------------------------
# JSON parse helper
# ---------------------------------------------------------------------------


def parse_llm_json(raw_text: str, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Extract and parse a JSON object from an LLM response.

    LLMs often wrap JSON in markdown fences or add preamble text.
    This function finds the first `{…}` block and parses it.

    Args:
        raw_text: The raw LLM response text.
        fallback: Dict to return if parsing fails. Defaults to empty dict.

    Returns:
        Parsed dict, or fallback on failure.
    """
    if fallback is None:
        fallback = {}

    start = raw_text.find("{")
    end = raw_text.rfind("}") + 1
    if start == -1 or end == 0:
        logger.warning("llm_json_not_found", raw=raw_text[:200])
        return fallback

    try:
        return json.loads(raw_text[start:end])
    except json.JSONDecodeError:
        logger.warning("llm_json_invalid", raw=raw_text[:200])
        return fallback


# ---------------------------------------------------------------------------
# Synchronous call wrapper
# ---------------------------------------------------------------------------


def call_openrouter_sync(
    *,
    model: str,
    messages: list[dict[str, Any]],
    max_tokens: int = 256,
    temperature: float = 0.1,
    api_key: str | None = None,
    timeout: float = 25.0,
) -> dict[str, Any]:
    """
    Make a synchronous call to the OpenRouter API and return the parsed response.

    Returns the full response JSON dict. Raises on HTTP errors.
    """
    headers = openrouter_headers(api_key)

    with httpx.Client(timeout=timeout) as client:
        resp = client.post(
            OPENROUTER_URL,
            headers=headers,
            json={
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
        )
        resp.raise_for_status()

    return resp.json()


def extract_content(response: dict[str, Any]) -> str:
    """Extract the text content from an OpenRouter API response."""
    return response["choices"][0]["message"]["content"].strip()
