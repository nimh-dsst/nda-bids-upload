import pandas as pd
import pytest
from bids import BIDSLayout
from tempfile import TemporaryDirectory
from utilities.lookup import LookUpTable

# Expected lookup table for bids-examples/pet002 (from running create_lookup_table).
EXPECTED_PET002_LOOKUP = pd.DataFrame([
    {"bids_subject_session": "sub-01_ses-baseline", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-01_ses-baseline", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-01_ses-baseline", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-01_ses-baseline", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-01_ses-rescan", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-01_ses-rescan", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-01_ses-rescan", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-01_ses-rescan", "subjectkey": "", "src_subject_id": "sub-01", "interview_date": "", "interview_age": 258.1032, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-02_ses-baseline", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-02_ses-baseline", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-02_ses-baseline", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-02_ses-baseline", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-02_ses-rescan", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-02_ses-rescan", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "anat"},
    {"bids_subject_session": "sub-02_ses-rescan", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "pet"},
    {"bids_subject_session": "sub-02_ses-rescan", "subjectkey": "", "src_subject_id": "sub-02", "interview_date": "", "interview_age": 248.7060, "sex": "F", "datatype": "pet"},
])


def test_load_bids_pet_data(bids_examples_petprep):
    """Load BIDS layout from pet002 (bids-examples submodule, includes NIfTIs)."""
    layout = BIDSLayout(str(bids_examples_petprep))
    assert layout is not None
    subjects = layout.get_subjects()
    assert len(subjects) >= 1


def test_lookup_table_with_pet_data(bids_pet_fixture):
    """LookUpTable from hardcoded participants matches expected output."""
    lut = LookUpTable(str(bids_pet_fixture))
    lut.create_lookup_table()
    actual = lut.lookup_table
    assert actual is not None
    # Sort for stable comparison (layout iteration order may vary)
    actual = actual.sort_values(["bids_subject_session", "datatype"]).reset_index(drop=True)
    expected = EXPECTED_PET002_LOOKUP.sort_values(["bids_subject_session", "datatype"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_exact=False, atol=1e-4, rtol=0)

def test_not_a_bids_dataset():
    tempdir = TemporaryDirectory()
    with pytest.raises(ValueError) as not_bids_dataset_error:
        nope = LookUpTable(tempdir)
    assert "BIDS root does not exist" in str(not_bids_dataset_error)

def test_missing_participants_tsv(bids_pet_fixture_fresh):
    """Creating the lookup table raises when participants.tsv is missing."""
    with pytest.raises(FileNotFoundError) as exc_info:
        new_table = LookUpTable(str(bids_pet_fixture_fresh))
        (bids_pet_fixture_fresh / "participants.tsv").unlink()
        new_table.create_lookup_table()
    assert "participants.tsv" in str(exc_info.value)
    
def test_missing_participants_json(bids_pet_fixture_fresh):
    """Creating the lookup table raises when participants.json is missing (hits open→except→raise)."""
    with pytest.raises(FileNotFoundError) as exc_info:
        new_table = LookUpTable(str(bids_pet_fixture_fresh))
        (bids_pet_fixture_fresh / "participants.json").unlink()
        new_table.create_lookup_table()
    assert "participants.json" in str(exc_info.value)


def test_age_units_years(bids_pet_fixture):
    """Units: years → age * 12 for interview_age in months."""
    lut = LookUpTable(str(bids_pet_fixture))
    lut.create_lookup_table()
    sub01 = lut.lookup_table[lut.lookup_table["src_subject_id"] == "sub-01"]
    assert len(sub01) > 0
    # 21.5086 years * 12 = 258.1032 months
    assert (sub01["interview_age"] - 258.1032).abs().max() < 1e-3


def test_age_units_months(bids_pet_fixture_months):
    """Units: months → age * 1 for interview_age in months."""
    lut = LookUpTable(str(bids_pet_fixture_months))
    lut.create_lookup_table()
    sub01 = lut.lookup_table[lut.lookup_table["src_subject_id"] == "sub-01"]
    assert len(sub01) > 0
    # TSV has 21.5086*12 months, multiplier 1 → 258.1032
    assert (sub01["interview_age"] - 258.1032).abs().max() < 1e-3


def test_age_units_weeks(bids_pet_fixture_weeks):
    """Units: weeks → age * (1/4) for interview_age in months."""
    lut = LookUpTable(str(bids_pet_fixture_weeks))
    lut.create_lookup_table()
    sub01 = lut.lookup_table[lut.lookup_table["src_subject_id"] == "sub-01"]
    assert len(sub01) > 0
    # TSV has 21.5086*52 weeks, multiplier 1/4 → 21.5086*13 = 279.61 months
    expected_months = 21.5086 * 52 / 4
    assert (sub01["interview_age"] - expected_months).abs().max() < 1e-3

def test_age_units_unknown_fires_else_branch(capsys, bids_pet_fixture_unknown_age_units):
    """Unknown age Units (e.g. 'days') hits the else branch and prints the warning."""
    lut = LookUpTable(str(bids_pet_fixture_unknown_age_units))
    lut.create_lookup_table()
    captured = capsys.readouterr()
    assert "unable to determine participant.age.Units" in captured.out
    assert "not converting to months" in captured.out


def test_write_lookup_table(bids_pet_fixture_fresh):
    """write_lookup_table creates CSV at destination_path; builds table if empty."""
    out_path = bids_pet_fixture_fresh / "lookup.csv"
    lut = LookUpTable(str(bids_pet_fixture_fresh), destination_path=str(out_path))
    assert lut.lookup_table.empty
    lut.write_lookup_table()
    assert out_path.exists()
    content = out_path.read_text()
    assert "bids_subject_session" in content
    assert "interview_age" in content
    assert "sub-01" in content
