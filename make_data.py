#!/usr/bin/env python3
"""Generate small synthetic RGB crop tiles under data/tiles/ plus data/tiles.json (metadata)."""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from pathlib import Path

from PIL import Image, ImageDraw

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'data' / 'tiles'
REGIONS = ['central-valley', 'po-valley', 'pampas', 'punjab']


def main(n: int = 48, seed: int = 7) -> None:
    rnd = random.Random(seed)
    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(n):
        region = REGIONS[i % len(REGIONS)]
        vigor = rnd.uniform(0.15, 1.0)  # 1.0 = lush field, 0.0 = bare soil
        r = int(150 - 90 * vigor + rnd.randint(-8, 8))
        g = int(110 + 90 * vigor + rnd.randint(-8, 8))
        b = int(80 - 30 * vigor + rnd.randint(-8, 8))
        img = Image.new('RGB', (256, 256), (r, g, b))
        d = ImageDraw.Draw(img)
        for k in range(0, 256, 16):  # crop rows
            d.line([(k, 0), (k, 255)], fill=(max(r - 12, 0), max(g - 12, 0), b), width=2)
        path = OUT / f'tile_{i:03d}.png'
        img.save(path)
        rows.append(
            {
                'tile_id': f'T{i:03d}',
                'field_id': f'F{i % 12:02d}',
                'region': region,
                'captured_on': (date(2026, 6, 1) + timedelta(days=i // 4)).isoformat(),
                'path': str(path.relative_to(ROOT)),
            }
        )
    (ROOT / 'data' / 'tiles.json').write_text(json.dumps(rows, indent=1))
    print(f'wrote {n} tiles')


if __name__ == '__main__':
    main()
