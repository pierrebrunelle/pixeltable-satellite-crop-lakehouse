"""Pixeltable UDFs for the satellite crop lakehouse pipeline (module path e.g. `udfs.greenness`)."""

from __future__ import annotations

import PIL.Image

import pixeltable as pxt


@pxt.udf
def greenness(img: PIL.Image.Image) -> float:
    """Excess-green index of a tile, scaled 0..1 (a cheap NDVI stand-in for RGB-only tiles)."""
    small = img.convert('RGB').resize((32, 32))
    px = list(small.getdata())
    exg = sum(2 * g - r - b for r, g, b in px) / (len(px) * 255.0)
    return max(0.0, min(1.0, (exg + 1.0) / 2.0))


@pxt.udf
def crop_health(green: float) -> str:
    """Bucket the greenness score into a health band."""
    if green >= 0.90:
        return 'vigorous'
    if green >= 0.78:
        return 'normal'
    if green >= 0.68:
        return 'stressed'
    return 'bare'
