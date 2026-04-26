from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Iterator, Optional


def read_csv_dict_rows(path: Path) -> Iterator[dict[str, Optional[str]]]:
    # Some broker exports contain multiple UTF-8 BOM markers at the file start.
    # Strip all leading BOM chars before csv parsing so headers match expected names.
    content = path.read_text(encoding="utf-8-sig").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        yield {(key.lstrip("\ufeff") if key is not None else key): value for key, value in row.items()}
