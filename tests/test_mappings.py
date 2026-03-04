"""Tests for file-mapper mapping JSON format and usage.

Mapping examples (from nda_ds004869) used in this module:

- image03_sourcedata.anat.t1w.json (anat, no session):
  {"CHANGES":"CHANGES","README":"README","dataset_description.json":"dataset_description.json",
   "LICENSE":"LICENSE",
   "sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.nii.gz":"sub-{GUID}/anat/sub-{GUID}_T1w.nii.gz",
   "sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.json":"sub-{GUID}/anat/sub-{GUID}_T1w.json"}

- image03_sourcedata.pet.pet.json (PET with session):
  {"CHANGES":"CHANGES","README":"README","dataset_description.json":"dataset_description.json",
   "LICENSE":"LICENSE",
   "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_pet.nii.gz":"sub-{GUID}/ses-{SESSION}/pet/...",
   "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_pet.json":"sub-{GUID}/ses-{SESSION}/pet/...",
   ... recording-manual_blood.json and .tsv ...}
"""

import pytest
import bids
import json
import yaml

from pathlib import Path
from utilities.mapping import create_jsons, create_yamls

# collect bids layout object from test data
def test_bids_example_json_mappings(pet002_copy):
    json_mapping_paths = create_jsons(pet002_copy.bids_dir, pet002_copy.upload_dir)
    assert len(json_mapping_paths) > 0
    assert all([Path(j).exists() for j in json_mapping_paths])
    for j in json_mapping_paths:
        with open(j, 'r') as infile:
            json_mapping = json.load(infile)
            assert type(json_mapping) is dict
    
def test_bids_example_yaml_mappings(pet002_copy):
    yaml_mapping_paths = create_yamls(pet002_copy.bids_dir, pet002_copy.upload_dir)
    assert len(yaml_mapping_paths) > 0
    assert all([Path(j).exists() for j in yaml_mapping_paths])
    for y in yaml_mapping_paths:
        with open(y, 'r') as infile:
            yaml_mapping = yaml.safe_load(infile)
            assert type(yaml_mapping) is dict