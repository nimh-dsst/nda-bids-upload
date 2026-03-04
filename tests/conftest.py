"""Pytest configuration and shared fixtures."""

from collections import namedtuple
import copy
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

# Hardcoded participants (pet002-style). TSV is written via pandas for readability.
PARTICIPANTS_DATA = [
    {"participant_id": "sub-01", "height": 163.5, "weight": 51, "age": 21.5086, "gender": "F"},
    {"participant_id": "sub-02", "height": 170, "weight": 51.2, "age": 20.7255, "gender": "F"},
]

PARTICIPANTS_JSON = {
    "participant_id": {"LongName": "Participant Id", "Description": "label identifying a particular subject"},
    "height": {"LongName": "Height", "Description": "Height of the participant", "Units": "cm"},
    "weight": {"LongName": "Weight", "Description": "Weight of the participant", "Units": "kg"},
    "age": {"LongName": "Age", "Description": "Age of the participant", "Units": "years"},
    "gender": {"LongName": "gender", "Description": "Sex of the participant", "Levels": {"M": "male", "F": "female"}},
}

# Same ages in months (for Units: "months" → multiplier 1)
PARTICIPANTS_DATA_MONTHS = [
    {"participant_id": "sub-01", "height": 163.5, "weight": 51, "age": 21.5086 * 12, "gender": "F"},
    {"participant_id": "sub-02", "height": 170, "weight": 51.2, "age": 20.7255 * 12, "gender": "F"},
]
PARTICIPANTS_JSON_MONTHS = copy.deepcopy(PARTICIPANTS_JSON)
PARTICIPANTS_JSON_MONTHS["age"]["Units"] = "months"

# Same ages in weeks (for Units: "weeks" → multiplier 1/4 to get months)
PARTICIPANTS_DATA_WEEKS = [
    {"participant_id": "sub-01", "height": 163.5, "weight": 51, "age": 21.5086 * 52, "gender": "F"},
    {"participant_id": "sub-02", "height": 170, "weight": 51.2, "age": 20.7255 * 52, "gender": "F"},
]
PARTICIPANTS_JSON_WEEKS = copy.deepcopy(PARTICIPANTS_JSON)
PARTICIPANTS_JSON_WEEKS["age"]["Units"] = "weeks"

# Unknown age Units so lookup hits the else branch (print "unable to determine...")
# Use a value with no "y", "m", or "w" (e.g. "days" contains "y" and would match years)
PARTICIPANTS_JSON_UNKNOWN_AGE_UNITS = copy.deepcopy(PARTICIPANTS_JSON)
PARTICIPANTS_JSON_UNKNOWN_AGE_UNITS["age"]["Units"] = ""

DATASET_DESCRIPTION = {"BIDSVersion": "1.6.0", "Name": "Test PET dataset", "License": "CC0"}


def _project_root() -> Path:
    """Project root (nda-bids-upload)."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def bids_examples_root() -> Path:
    """Path to the bids-examples directory (submodule)."""
    root = _project_root() / "bids-examples"
    if not root.is_dir():
        pytest.skip("bids-examples not found (submodule may not be initialized)")
    return root


@pytest.fixture(scope="session")
def bids_examples_petprep(bids_examples_root: Path) -> Path:
    """Path to PET data in bids-examples (pet002)."""
    path = bids_examples_root / "pet002"
    if not path.is_dir():
        pytest.skip("bids-examples/pet002 not found")
    return path


def _build_bids_pet_root(
    root: Path,
    participants_data: list = None,
    participants_json: dict = None,
) -> None:
    """Fill a directory with minimal BIDS PET layout. Defaults to PARTICIPANTS_DATA / PARTICIPANTS_JSON."""
    if participants_data is None:
        participants_data = PARTICIPANTS_DATA
    if participants_json is None:
        participants_json = PARTICIPANTS_JSON
    pd.DataFrame(participants_data).to_csv(root / "participants.tsv", sep="\t", index=False)
    (root / "participants.json").write_text(json.dumps(participants_json, indent=2))
    (root / "dataset_description.json").write_text(json.dumps(DATASET_DESCRIPTION, indent=2))
    for sub in ("01", "02"):
        for ses in ("baseline", "rescan"):
            for datatype, suffix in (("anat", "T1w"), ("pet", "pet")):
                d = root / f"sub-{sub}" / f"ses-{ses}" / datatype
                d.mkdir(parents=True)
                for run in (1, 2):
                    (d / f"sub-{sub}_ses-{ses}_run-0{run}_{suffix}.json").write_text("{}")


@pytest.fixture(scope="session")
def bids_pet_fixture(tmp_path_factory):
    """Minimal BIDS PET dataset from hardcoded participants (session-scoped)."""
    root = tmp_path_factory.mktemp("bids_pet")
    _build_bids_pet_root(root)
    return root


@pytest.fixture
def bids_pet_fixture_fresh(tmp_path):
    """Same as bids_pet_fixture but function-scoped so each test gets a clean dir (for mutation tests)."""
    _build_bids_pet_root(tmp_path)
    return tmp_path


@pytest.fixture
def bids_pet_fixture_months(tmp_path):
    """BIDS PET fixture with participants age in months (Units: months → multiplier 1)."""
    _build_bids_pet_root(tmp_path, PARTICIPANTS_DATA_MONTHS, PARTICIPANTS_JSON_MONTHS)
    return tmp_path


@pytest.fixture
def bids_pet_fixture_weeks(tmp_path):
    """BIDS PET fixture with participants age in weeks (Units: weeks → multiplier 1/4)."""
    _build_bids_pet_root(tmp_path, PARTICIPANTS_DATA_WEEKS, PARTICIPANTS_JSON_WEEKS)
    return tmp_path


@pytest.fixture
def bids_pet_fixture_unknown_age_units(tmp_path):
    """BIDS PET fixture with age Units that don't match y/m/w (triggers else-branch print)."""
    _build_bids_pet_root(tmp_path, PARTICIPANTS_DATA, PARTICIPANTS_JSON_UNKNOWN_AGE_UNITS)
    return tmp_path


@pytest.fixture
def pet002_copy(tmp_path, bids_examples_petprep):
    """Fresh copy of bids-examples/pet002 into a temp dir (function-scoped, for mapping tests)."""
    bids_dir = tmp_path / "pet002"
    shutil.copytree(bids_examples_petprep, bids_dir)
    upload_dir = tmp_path / "upload_dir"
    upload_dir.mkdir()
    TestDataset = namedtuple("TestDataset",['bids_dir', 'upload_dir'])
    return TestDataset(bids_dir=bids_dir, upload_dir=upload_dir)
