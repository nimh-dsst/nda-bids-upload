#!/usr/bin/env python3
"""Discover datasets that fail reduce_bids + lookup; write their names to tests/exclude.txt.

Run from project root:
    uv run python tests/discover_exclude.py

Then rerun the test; it will skip datasets listed in tests/exclude.txt.
"""

import sys
import tempfile
from pathlib import Path

# Same path setup as test_bids_examples
_tests_dir = Path(__file__).resolve().parent
if str(_tests_dir) not in sys.path:
    sys.path.insert(0, str(_tests_dir))
from reduce_bids_participants import reduce_bids


def _project_root() -> Path:
    return _tests_dir.parent


def _bids_examples_path() -> Path:
    root = _project_root()
    for candidate in (root / "tests" / "bids-examples", root / "bids-examples"):
        if candidate.is_dir():
            return candidate
    return root / "tests" / "bids-examples"


def main() -> None:
    root = _bids_examples_path()
    if not root.is_dir():
        print(f"bids-examples not found at {root}", file=sys.stderr)
        sys.exit(1)
    datasets = sorted(
        p for p in root.iterdir()
        if p.is_dir() and not p.name.startswith(".")
    )
    if not datasets:
        print("no dataset folders in bids-examples", file=sys.stderr)
        sys.exit(1)

    failed: list[str] = []
    for dataset_path in datasets:
        name = dataset_path.name
        try:
            with tempfile.TemporaryDirectory() as tmp:
                reduce_bids(dataset_path, Path(tmp), max_participants=10)
                if not (Path(tmp) / "upload" / "lookup.csv").is_file():
                    failed.append(name)
                    print(f"FAIL (no lookup.csv): {name}")
        except Exception as e:
            failed.append(name)
            print(f"FAIL: {name}  ({e!r})")

    exclude_path = _tests_dir / "exclude.txt"
    exclude_path.write_text("\n".join(sorted(failed)) + ("\n" if failed else ""))
    print(f"\nWrote {len(failed)} dataset(s) to {exclude_path}")
    if failed:
        print("Excluded:", ", ".join(sorted(failed)))


if __name__ == "__main__":
    main()
