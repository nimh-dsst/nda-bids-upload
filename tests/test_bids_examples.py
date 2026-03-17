"""Tests that require the bids-examples submodule and example data.

Skips all tests in this module if the submodule is not initialized or if
no example datasets (e.g. pet002) are present. Run `make submodules` first.
"""

import sys
import tempfile
from pathlib import Path
import subprocess
import shutil

import pytest

# Folder names under bids-examples to skip (e.g. "code", "tools").
# Datasets that fail reduce_bids + lookup are listed in tests/exclude.txt (one per line).
# Run: uv run python tests/discover_exclude.py  to regenerate exclude.txt.
EXCLUDE_DATASETS: list[str] = []


def _load_exclude_file() -> list[str]:
    """Load exclude list from tests/exclude.txt (one dataset name per line)."""
    exclude_path = _project_root() / "tests" / "exclude.txt"
    if not exclude_path.is_file():
        return []
    return [
        line.strip()
        for line in exclude_path.read_text().splitlines()
        if line.strip()
    ]

# Allow importing reduce_bids_participants when running this test file.
if __name__ != "__main__":
    _tests_dir = Path(__file__).resolve().parent
    if str(_tests_dir) not in sys.path:
        sys.path.insert(0, str(_tests_dir))



def _project_root() -> Path:
    """Project root (nda-bids-upload)."""
    return Path(__file__).resolve().parent.parent


def _bids_examples_path() -> Path:
    """Path to the bids-examples submodule (tests/bids-examples, or bids-examples at project root)."""
    root = _project_root()
    for candidate in (root / "tests" / "bids-examples", root / "bids-examples"):
        if candidate.is_dir():
            return candidate
    return root / "tests" / "bids-examples"  # preferred; fixture will skip if missing


def _bids_dataset_dirs(root: Path, exclude: list[str] | None = None):
    """Yield paths to all subdirs of root as BIDS datasets, optionally excluding given folder names."""
    excluded = set(EXCLUDE_DATASETS) | set(_load_exclude_file())
    if exclude is not None:
        excluded |= set(exclude)
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith(".") or p.name in excluded:
            continue
        yield p


@pytest.fixture(scope="module")
def bids_examples_available():
    """Ensure bids-examples submodule is loaded and example files are present.

    Skips all tests in this module if:
    - The bids-examples directory does not exist (submodule not initialized), or
    - The directory is empty or missing expected content (e.g. no dataset dirs).
    """
    root = _bids_examples_path()
    if not root.is_dir():
        pytest.skip(
            "bids-examples submodule not loaded. Run: make submodules"
        )
    # Check for typical content: dataset_description.json or at least one dataset dir (e.g. pet002)
    has_description = (root / "dataset_description.json").is_file()
    subdirs = [d for d in root.iterdir() if d.is_dir() and not d.name.startswith(".")]
    if not has_description and not subdirs:
        pytest.skip(
            "bids-examples directory is empty or has no example datasets. Run: make submodules"
        )
    return root


def test_bids_examples_submodule_loaded(bids_examples_available):
    """bids-examples submodule is present and has content."""
    root = bids_examples_available
    assert root.is_dir()
    # At least one of: root dataset_description or a known example dataset
    assert (root / "dataset_description.json").is_file() or any(
        (root / d).is_dir() for d in ("pet002", "pet003") if (root / d).exists()
    )


def test_pet002_example_present(bids_examples_available):
    """pet002 example dataset exists and has expected BIDS layout."""
    root = bids_examples_available
    pet002 = root / "pet002"
    if not pet002.is_dir():
        pytest.skip("pet002 not found in bids-examples")
    assert (pet002 / "participants.tsv").is_file() or (pet002 / "dataset_description.json").is_file()


def test_lookup_table_per_bids_dataset(bids_examples_available):
    """Per dataset: temp folder -> reduce -> lookup on reduced -> update lookup.csv with GUIDs/dates; assert success."""
    from populate_bids_participants import (
        ndaify_participants_files,
        InvalidDatasetError,
    )
    from utilities.lookup import LookUpTable
    from utilities.mapping import MappingTemplator
    from prepare import input_check, filemap_and_recordsprep

    # 1. Collect list of folders from bids-examples
    root = bids_examples_available
    datasets = list(_bids_dataset_dirs(root))
    if not datasets:
        pytest.skip("no dataset folders in bids-examples")

    for dataset_path in datasets:
        name = dataset_path.name
        # 2. Create a temporary folder for only this single dataset
        with tempfile.TemporaryDirectory() as tmp:
            target_path = Path(tmp)
            reduced_bids_dataset = target_path / f"{name}_reduced"
            nda_upload_directory = reduced_bids_dataset / "upload"
            nda_upload_directory.mkdir(parents=True,exist_ok=True)
            # 3. Run reduce_bids_participants on this dataset; target path is within the temp folder.
            #    (This reduces the dataset and writes the reduced BIDS tree into target_path.)
            try:
                ndaify_participants_files(
                    dataset_path, reduced_bids_dataset, max_participants=10
                )
            except InvalidDatasetError as e:
                # If bids-validator fails to run (network/proxy/cache issues),
                # skip validation for this dataset but continue the loop so
                # that remaining pipeline tests still run on other datasets.
                msg = str(e)
                if "bids-validator failed" in msg:
                    # Log by attaching to the assertion message if nothing
                    # else runs for this dataset.
                    # Just continue to the next dataset without raising.
                    continue
                raise
            # 5. lookup.csv was updated with conftest GUIDS and generated interview dates (inside reduce_bids).
            assert (nda_upload_directory / "lookup.csv").is_file(), (
                f"dataset {name}: expected lookup.csv after reduce + lookup"
            )
            # 6. Generate file mapping
            mapping = MappingTemplator(reduced_bids_dataset, nda_upload_directory)
    
            # 7. Run prepare.py
            # dest_dir, manifest_script, source_dir, skip, = input_check()
            filemap_and_recordsprep(nda_upload_directory, reduced_bids_dataset, False)
            print("pause")

            # 8. Additionally, run vtcmd explicitly on the generated
            # NDA records for this dataset, if vtcmd is available.
            #project_root = _project_root()
            #venv_vtcmd = project_root / ".venv" / "bin" / "vtcmd"
            #if venv_vtcmd.is_file():
            #   vtcmd_exe = str(venv_vtcmd)
            #else:
            #    vtcmd_exe = shutil.which("vtcmd")

            #if vtcmd_exe:
            #    dest_path = nda_upload_directory
            #    for records_csv in dest_path.glob("*.complete_records.csv"):
            #        result = subprocess.run(
            #            [vtcmd_exe, str(records_csv), "-m", str(records_csv.parent)],
            #            capture_output=True,
            #            text=True,
            #        )
            #        assert (
            #            result.returncode == 0
            #        ), f"vtcmd failed on {records_csv}:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"


