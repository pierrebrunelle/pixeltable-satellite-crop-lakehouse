"""Satellite crop imagery lakehouse pipeline built with Pixeltable.

An agronomy team receives small RGB satellite tiles per field. Each tile is stored in
one table, scored incrementally with computed columns (greenness, health band, a
thumbnail), and then exported in three lakehouse formats: Parquet, Apache Iceberg and
LanceDB. Several writer threads load tiles in parallel and the pipeline prints timings.
No HTTP router.

    pxt schema update app.py crops
    python make_data.py
    python pipeline.py --writers 4
"""

from __future__ import annotations

import pixeltable as pxt
import pixeltable.functions as pxtf

from udfs import crop_health, greenness

TableModel = pxt.model_base()


class Tiles(TableModel, name='tiles', has_default_idxs=False):
    """One satellite tile (RGB image) for a field and capture date."""

    tile_id = pxt.Column(type=pxt.String, primary_key=True)
    field_id: pxt.String
    region: pxt.String
    captured_on: pxt.Date
    image: pxt.Image

    width = image.width
    height = image.height
    green = greenness(image)
    health = crop_health(green)
    thumb = pxtf.image.resize(image, (64, 64))

    __indexes__ = [pxt.BtreeIndex(field_id), pxt.BtreeIndex(region)]


@pxt.query
def stressed_fields(region: str, max_green: float):
    """Tiles in a region whose greenness is at or below a threshold, worst first."""
    return (
        Tiles.where((Tiles.region == region) & (Tiles.green <= max_green))
        .order_by(Tiles.green)
        .select(Tiles.tile_id, Tiles.field_id, Tiles.captured_on, Tiles.green, Tiles.health)
    )
