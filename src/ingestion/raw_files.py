from __future__ import annotations

from pathlib import Path


RAW_FILE_SUFFIX_PREFERENCE = {
    ".pdf": 0,
    ".csv": 1,
    ".txt": 2,
}


def find_preferred_raw_files(root: Path, prefix: str) -> list[Path]:
    selected: dict[str, Path] = {}
    for suffix in RAW_FILE_SUFFIX_PREFERENCE:
        for path in root.glob(f"{prefix}_*_*{suffix}"):
            existing = selected.get(path.stem)
            if existing is None:
                selected[path.stem] = path
                continue

            existing_rank = RAW_FILE_SUFFIX_PREFERENCE[existing.suffix.lower()]
            candidate_rank = RAW_FILE_SUFFIX_PREFERENCE[path.suffix.lower()]
            if candidate_rank < existing_rank:
                selected[path.stem] = path

    return sorted(selected.values())
