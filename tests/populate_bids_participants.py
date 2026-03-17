#!/usr/bin/env python3
"""
Reduce a BIDS dataset (including derivatives) to at most N participants.

Uses pybids to discover subjects and files. Copies only data for the selected
participants and rewrites participants.tsv (and participants.json if present)
in each dataset and derivative to include only those participants.

All file creation (reduced dataset and nda-lookup table) occurs in a
temporary directory; results are then copied to the output path.

Validates input and output with the Deno bids-validator (must be on PATH),
using bids-examples/default-config.json and --ignoreNiftiHeaders.

Usage:
    python reduce_bids_participants.py /path/to/bids_root /path/to/output [--max-participants 10]
"""

import argparse
import importlib.util
import json
import random
import shutil
import subprocess
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from bids import BIDSLayout

from utilities.lookup import LookUpTable


def _load_conftest_guids() -> list[str]:
    """Load GUIDS from tests/conftest.py (same package as this script)."""
    conftest_path = Path(__file__).resolve().parent / "conftest.py"
    spec = importlib.util.spec_from_file_location("conftest", conftest_path)
    conftest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conftest)
    return getattr(conftest, "GUIDS", [])


class InvalidDatasetError(Exception):
    """Raised when bids-validator reports a non-zero exit (invalid BIDS dataset)."""


def _project_root() -> Path:
    """Project root (nda-bids-upload)."""
    return Path(__file__).resolve().parent.parent


def _default_validator_config() -> Path:
    """Path to default validator config (bids-examples/default-config.json)."""
    return _project_root() / "bids-examples" / "default-config.json"


def _run_bids_validator(
    dataset_path: Path,
    config_path: Path,
) -> None:
    """Run Deno bids-validator on dataset; raise InvalidDatasetError if exit code is not 0."""
    cmd = [
        "bids-validator",
        str(dataset_path),
        "--config",
        str(config_path),
        "--ignoreNiftiHeaders",
        "--ignoreWarnings",
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise InvalidDatasetError(
            f"bids-validator failed (exit {result.returncode}) on {dataset_path}\n"
            f"stdout:\n{result.stdout or '(none)'}\n"
            f"stderr:\n{result.stderr or '(none)'}"
        )


def _normalize_participant_id(subject_id: str) -> str:
    """Return BIDS participant_id form (sub-XX)."""
    s = str(subject_id).strip()
    if s.startswith("sub-"):
        return s
    return f"sub-{s}"


def _get_keep_subjects(layout: BIDSLayout, max_participants: int) -> list[str]:
    """Return up to max_participants subject IDs (normalized) to keep."""
    all_subjects = layout.get_subjects()
    if not all_subjects:
        return []
    # Deterministic order
    unique = sorted(set(str(s) for s in all_subjects))
    keep = unique[:max_participants]
    return [_normalize_participant_id(s) for s in keep]


def _copy_file(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def _filter_and_write_participants_tsv(
    src_tsv: Path, dest_tsv: Path, keep_ids: list[str]
) -> None:
    """Read participants.tsv, keep only rows for keep_ids, write to dest."""
    df = pd.read_csv(src_tsv, sep="\t", dtype=str)
    if "participant_id" not in df.columns:
        # No participant column; copy as-is (empty or non-standard)
        _copy_file(src_tsv, dest_tsv)
        return
    filtered = df[df["participant_id"].isin(keep_ids)]
    dest_tsv.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(dest_tsv, sep="\t", index=False)


def _ensure_participants_age(dataset_dir: Path) -> None:
    """
    Add an age column to participants.tsv if missing (values 1–100).
    If age was added, update participants.json with the age field schema.
    """
    tsv_path = dataset_dir / "participants.tsv"
    json_path = dataset_dir / "participants.json"
    if not tsv_path.is_file():
        return
    df = pd.read_csv(tsv_path, sep="\t", dtype=str)
    if "age" in df.columns:
        return
    n = len(df)
    df["age"] = [str(random.randint(1, 100)) for _ in range(n)]
    df.to_csv(tsv_path, sep="\t", index=False)
    if not json_path.is_file():
        return
    with open(json_path) as f:
        meta = json.load(f)
    meta["age"] = {
        "Description": "age of the participant",
        "Units": "year",
    }
    with open(json_path, "w") as f:
        json.dump(meta, f, indent=2)


def _ensure_participants_sex(dataset_dir: Path) -> None:
    """
    Add a sex column to participants.tsv if missing (values M or F at random).
    If sex was added, update participants.json with the sex field schema.
    """
    tsv_path = dataset_dir / "participants.tsv"
    json_path = dataset_dir / "participants.json"
    if not tsv_path.is_file():
        return
    df = pd.read_csv(tsv_path, sep="\t", dtype=str)
    if "sex" in df.columns:
        return
    n = len(df)
    df["sex"] = [random.choice(["M", "F"]) for _ in range(n)]
    df.to_csv(tsv_path, sep="\t", index=False)
    if not json_path.is_file():
        return
    with open(json_path) as f:
        meta = json.load(f)
    meta["sex"] = {
        "Description": "sex of the participant as reported by the participant",
        "Levels": {
            "M": "male",
            "F": "female",
        },
    }
    with open(json_path, "w") as f:
        json.dump(meta, f, indent=2)


def _dataset_roots(bids_root: Path) -> list[Path]:
    """Return main dataset root and each derivative pipeline root."""
    roots = [bids_root]
    deriv_dir = bids_root / "derivatives"
    if deriv_dir.is_dir():
        for name in sorted(deriv_dir.iterdir()):
            if name.is_dir() and not name.name.startswith("."):
                roots.append(name)
    return roots


def _copy_dataset_level_files(
    input_root: Path, output_root: Path, keep_ids: list[str]
) -> None:
    """Copy dataset_description, participants (filtered), README for root and derivatives."""
    for root in _dataset_roots(input_root):
        try:
            rel = root.relative_to(input_root)
        except ValueError:
            rel = root.name
        out_dir = output_root / rel
        for name in ("dataset_description.json", "participants.json", "README", "README.md"):
            src = root / name
            if src.is_file():
                _copy_file(src, out_dir / name)
        participants_tsv = root / "participants.tsv"
        if participants_tsv.is_file():
            _filter_and_write_participants_tsv(
                participants_tsv, out_dir / "participants.tsv", keep_ids
            )


def _populate_lookup_guids_and_dates(
    df: pd.DataFrame, keep_ids: list[str], guids: list[str], base_date: date | None = None
) -> pd.DataFrame:
    """Fill subjectkey from guids (by subject index in keep_ids) and interview_date as mm/dd/yyyy."""
    if base_date is None:
        base_date = date(2020, 1, 1)
    subject_to_idx = {sid: i for i, sid in enumerate(keep_ids)}
    subjectkey = df["src_subject_id"].map(
        lambda s: guids[subject_to_idx[s]] if s in subject_to_idx and subject_to_idx[s] < len(guids) else ""
    )
    interview_date = df["src_subject_id"].map(
        lambda s: (base_date + timedelta(days=subject_to_idx.get(s, 0))).strftime("%m/%d/%Y")
        if s in subject_to_idx
        else ""
    )
    out = df.copy()
    out["subjectkey"] = subjectkey
    out["interview_date"] = interview_date
    return out


def _run_nda_lookup(
    bids_root: Path, upload_destination: Path, keep_ids: list[str]
) -> None:
    """Create lookup table for the BIDS dataset; fill subjectkey (from conftest GUIDS) and interview_date."""
    upload_destination.mkdir(parents=True, exist_ok=True)
    guids = _load_conftest_guids()
    lut = LookUpTable(str(bids_root), destination_path=str(upload_destination))
    lut.create_lookup_table()
    lut.lookup_table = _populate_lookup_guids_and_dates(
        lut.lookup_table, keep_ids, guids
    )
    lut.write_lookup_table()


def ndaify_participants_files(
    input_bids_root: str | Path,
    output_bids_root: str | Path,
    max_participants: int = 10,
    validator_config: Path | None = None,
    run_bids_validator: bool = False
) -> None:
    """
    Copy BIDS dataset (and derivatives) to output with at most max_participants.

    participants.tsv in each dataset/derivative is filtered to those participants.
    All file creation (reduced BIDS + nda-lookup table) is done in a temp dir,
    then copied to output_bids_root (reduced dataset) and output_bids_root/upload
    (lookup table). Validates input and output with bids-validator (Deno, on PATH).
    """
    input_root = Path(input_bids_root).resolve()
    output_root = Path(output_bids_root).resolve()
    if not input_root.is_dir():
        raise NotADirectoryError(f"Input BIDS root is not a directory: {input_root}")

    config_path = Path(validator_config) if validator_config else _default_validator_config()
    if not config_path.is_file():
        raise FileNotFoundError(f"Validator config not found: {config_path}")
    if run_bids_validator:
        _run_bids_validator(input_root, config_path)

    layout = BIDSLayout(
        str(input_root),
        validate=False,
        derivatives=True,
    )
    keep_ids = _get_keep_subjects(layout, max_participants)
    if not keep_ids:
        raise ValueError("No subjects found in the BIDS layout.")

    # Pybids get_subjects() returns "01", "02"; entity "subject" is same
    keep_subject_entities = sorted(
        set(s.replace("sub-", "") if s.startswith("sub-") else s for s in keep_ids)
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        reduced_root = tmp_path / "bids_reduced"
        reduced_root.mkdir(parents=True)
        upload_dir = tmp_path / "upload"

        # Copy subject-specific files from main and all derivatives (scope='all')
        try:
            files = layout.get(
                return_type="file",
                subject=keep_subject_entities,
                scope="all",
            )
        except TypeError:
            files = layout.get(return_type="file", scope="all")
            files = [
                f for f in files
                if getattr(f, "entities", {}).get("subject") in keep_subject_entities
            ]

        for f in files:
            src = Path(getattr(f, "path", f))
            if not src.is_file():
                continue
            try:
                rel = src.relative_to(layout.root)
            except ValueError:
                rel = src.relative_to(input_root)
            dest = reduced_root / rel
            _copy_file(src, dest)

        # Copy dataset-level files and write filtered participants.tsv
        _copy_dataset_level_files(input_root, reduced_root, keep_ids)

        # Ensure any participants.tsv under derivatives is also filtered
        for participants_tsv in input_root.rglob("participants.tsv"):
            try:
                rel = participants_tsv.relative_to(input_root)
            except ValueError:
                continue
            dest_tsv = reduced_root / rel
            if dest_tsv.exists():
                _filter_and_write_participants_tsv(participants_tsv, dest_tsv, keep_ids)
                _ensure_participants_age(dest_tsv.parent)
                _ensure_participants_sex(dest_tsv.parent)

        if run_bids_validator:
            _run_bids_validator(reduced_root, config_path)

        _run_nda_lookup(reduced_root, upload_dir, keep_ids)

        # Copy from temp to final output
        if output_root.exists():
            shutil.rmtree(output_root)
        shutil.copytree(reduced_root, output_root)
        shutil.copytree(upload_dir, output_root / "upload")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reduce a BIDS dataset (including derivatives) to at most N participants."
    )
    parser.add_argument(
        "input_bids",
        type=Path,
        help="Path to the input BIDS dataset root",
    )
    parser.add_argument(
        "output_bids",
        type=Path,
        help="Path to the output BIDS dataset root (will be created)",
    )
    parser.add_argument(
        "--max-participants",
        type=int,
        default=10,
        metavar="N",
        help="Maximum number of participants to keep (default: 10)",
    )
    parser.add_argument(
        "--validator-config",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to bids-validator config JSON (default: bids-examples/default-config.json)",
    )
    args = parser.parse_args()
    if args.max_participants < 1:
        parser.error("--max-participants must be at least 1")
    reduce_bids(
        args.input_bids,
        args.output_bids,
        max_participants=args.max_participants,
        validator_config=args.validator_config,
    )
    print(
        f"Wrote reduced dataset to {args.output_bids} (up to {args.max_participants} participants), "
        f"lookup table to {args.output_bids}/upload/lookup.csv"
    )


if __name__ == "__main__":
    main()
