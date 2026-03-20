"""
DealHunter — Image Utilities
Shared image processing helpers for validation pipelines.

Used by: product_image_selector.
"""

from __future__ import annotations

import base64
from io import BytesIO


def resize_for_validation(
    image_bytes: bytes,
    max_side: int = 512,
    quality: int = 70,
) -> str:
    """
    Resize an image for LLM validation and return as base64 JPEG.

    Normalizes the image to RGB, shrinks to max_side pixels on the longest
    dimension, and encodes as JPEG with the given quality.

    Args:
        image_bytes: Raw image bytes (any PIL-supported format).
        max_side: Maximum pixel size for the longest dimension.
        quality: JPEG compression quality (1-100).

    Returns:
        Base64-encoded JPEG string ready for LLM APIs.
    """
    from PIL import Image

    img = Image.open(BytesIO(image_bytes))
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    longest = max(img.size)
    if longest > max_side:
        ratio = max_side / longest
        img = img.resize(
            (int(img.width * ratio), int(img.height * ratio)),
            Image.LANCZOS,
        )

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=quality)
    return base64.b64encode(buf.getvalue()).decode()
