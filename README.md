# Welcome to the NIMH DSST NDA BIDS Upload Repository

This repository is for taking data as BIDS and uploading it to an NDA collection.

The full documentation lives here: [ndabids.readthedocs.io](https://ndabids.readthedocs.io/)

## Quick Start

### Installation

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/nimh-dsst/nda-bids-upload.git
cd nda-bids-upload

# Install with uv (recommended)
uv pip install -e .

# Or with pip
pip install -e .
```

For detailed installation instructions, see [INSTALL.md](INSTALL.md).

### Usage

1. Create a lookup.csv file to map BIDS subject/sessions to NDA GUID's

    ```bash
    # Run the script directly
    python -m utilities.lookup <source_dir> <destination_dir>

    # Or use the installed command
    nda-lookup <source_dir> <destination_dir>
    ```

    If you have your GUID's ready and want to enter them immediately you can append the `--edit-now` flag to the above
    command:

    ```bash
    nda-lookup <source_dir> <destination_dir> --edit-now
    ```

    ![image of lookup.csv editor](images/lookup_csv_editor.png)

2. Create file mapper json's and NDA Yaml files:

    ```bash
    # Run the script directly
    python -m utilities.mapping <source_dir> <destination_dir>

    # Or use the installed command
    nda-mapping <source_dir> <destination_dir>
    ```

    This produces mapper JSONs and YAMLs per BIDS datatype (e.g. anat, pet) and also
    **image03_sourcedata.bids.toplevel** (top-level BIDS files only: README, dataset_description.json,
    participants.tsv, etc.) for NDA image03 upload.

3. Use file mapper and nda manifests tool to create packages for uploading to NDA:

    ```bash
    # Prepare data for NDA upload
    python prepare.py -s <source_dir> -d <destination_dir>

    # Or use the installed command
    nda-prepare -s <source_dir> -d <destination_dir>
    ```

## Dependencies

This package includes:
- **[NDAR/manifest-data](https://github.com/NDAR/manifest-data)**: Git submodule providing the `nda_manifests.py` script
- **[bendhouseart/file-mapper](https://github.com/bendhouseart/file-mapper)**: Git dependency for file mapping operations
- Standard Python packages: mkdocs-material, PyYAML, pandas

All dependencies are managed via `pyproject.toml` for easy installation with `uv` or `pip`.
