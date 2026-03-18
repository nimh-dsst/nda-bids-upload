"""File-mapper JSON mapping definitions (source path → destination path).

Mappings are prepared manually and consumed by prepare.py with the file-mapper library.
Keys are paths under the source (BIDS-style with {SUBJECT}, {SESSION}); values are
paths under the destination (often NDA-style with {GUID}).

Example (anat T1w, no session) from nda_ds004869 image03_sourcedata.anat.t1w.json:

    {
        "sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.nii.gz": "sub-{GUID}/anat/sub-{GUID}_T1w.nii.gz",
        "sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.json": "sub-{GUID}/anat/sub-{GUID}_T1w.json"
    }

Example (PET with session) from nda_ds004869 image03_sourcedata.pet.pet.json:

    {
        "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_pet.nii.gz": "sub-{GUID}/ses-{SESSION}/pet/sub-{GUID}_ses-{SESSION}_pet.nii.gz",
        "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_pet.json": "sub-{GUID}/ses-{SESSION}/pet/sub-{GUID}_ses-{SESSION}_pet.json",
        "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_recording-manual_blood.json": "sub-{GUID}/ses-{SESSION}/pet/sub-{GUID}_ses-{SESSION}_recording-manual_blood.json",
        "sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_recording-manual_blood.tsv": "sub-{GUID}/ses-{SESSION}/pet/sub-{GUID}_ses-{SESSION}_recording-manual_blood.tsv"
    }
"""

from copy import copy

from pandas._libs.lib import fast_multiget
import bids
import re
import yaml
import json

from copy import copy
from typing import Union
from pathlib import Path
from argparse import ArgumentParser

supported_bids_datatypes = ["anat", "pet"]


class MappingTemplator:
    def __init__(
        self,
        bids_dataset: Union[bids.BIDSLayout, Path, str],
        destination_path: Union[Path, str] = "",
    ):
        if type(bids_dataset) is not bids.BIDSLayout:
            self.bids_layout = bids.BIDSLayout(bids_dataset)
        else:
            self.bids_layout = bids_dataset
        self.bids_dataset_path = Path(self.bids_layout.root)
        if destination_path == "" or destination_path is None:
            self.destination_path = self.bids_dataset_path.with_name(self.bids_dataset_path.name  + "_nda_upload"
            )
        else:
            self.destination_path = destination_path
        
        # create destination path if it does not exist
        Path(self.destination_path).mkdir(exist_ok=True, parents=True)

        self.datatypes = self.bids_layout.get_datatypes()
        self.subject_mappings = {
            subject: {datatype: [] for datatype in self.datatypes}
            for subject in self.bids_layout.get_subjects()
        }
        self.general_mappings = {modality: [] for modality in self.datatypes}
        self.finished_product = {modality: {} for modality in self.datatypes}
        self.populate_subject_mappings()
        self.aggregate_mappings()
        self.destination_path = destination_path
        # ultimately we'll want to support these as well: CT; SPECT; ultrasound; FA; X-Ray; spectroscopy; microscopy; DEXA; fNIRS; External Camera Photography
        self.bids_to_nda_image = {"anat": "MRI", "pet": "PET"}
        self.nda_file_descriptor = "image03_sourcedata.{}.{}"

        self.create_jsons()
        self.create_yamls()
        self.create_toplevel_json()
        self.create_toplevel_yaml()

    def create_toplevel_json(self):
        """Create identity mapping for BIDS dataset top-level files only."""
        root = Path(self.bids_dataset_path)
        if not root.is_dir():
            return
        toplevel = {}
        for p in root.iterdir():
            if p.is_file():
                name = p.name
                toplevel[name] = name
        if not toplevel:
            return
        out_path = Path(self.destination_path) / "image03_sourcedata.bids.toplevel.json"
        with open(out_path, "w") as f:
            json.dump(toplevel, f, indent=4)

    def create_toplevel_yaml(self):
        """Create content YAML for image03_sourcedata.bids.toplevel."""
        template = {
            "image_description": "bids toplevel",
            "scan_type": "BIDS dataset metadata",
            "scan_object": "Other",
            "image_modality": "Other",
            "transformation_performed": "No",
            "image_file_format": "DICOM",
        }
        out_path = Path(self.destination_path) / "image03_sourcedata.bids.toplevel.yaml"
        with open(out_path, "w") as f:
            yaml.dump(template, f)

    def populate_subject_mappings(self):
        for subject, datatype in self.subject_mappings.items():
            for d in datatype.keys():
                self.subject_mappings[subject][d] = [
                    re.sub(f"sub-{subject}", "sub-{SUBJECT}", file.relpath)
                    for file in self.bids_layout.get(subject=subject, datatype=d)
                ]

    def aggregate_mappings(self):
        """
        Collects ever bids subject/session and organizes them by subject, datatype,
        and file. These set of unique bids file paths will be used to create the 
        jsons for filemapper to symlink the nda datastructure from the BIDS dataset.

        For subject sub-example with a set of pet images will return the following:
        Transforms self.subject_mappings ->
        {
            'example': {
                'pet': [
                    'sub-example_session-a_pet.nii.gz',
                    'sub-example_session-a_pet.json',
                ],
                'anat': [
                    'sub-example_T1w.nii.gz',
                    'sub-example_T1w.json'
                ]
            }
        }

        To

        self.finished_product = {
            'anat': {
                'sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.nii.gz': 'sub-{GUID}/anat/sub-{GUID}_T1w.nii.gz',
                'sub-{SUBJECT}/anat/sub-{SUBJECT}_T1w.json': 'sub-{GUID}/anat/sub-{GUID}_T1w.json'
            },
            'pet': {
                'sub-{SUBJECT}/ses-{SESSION}/pet/sub-{SUBJECT}_ses-{SESSION}_pet.nii.gz': 'sub-{GUID}/ses-{SESSION}/pet/...'
            }
        }
        """
        for subject, datatype in self.subject_mappings.items():
            for d in datatype.keys():
                self.general_mappings[d].extend(self.subject_mappings[subject][d])
        # create a set of all unique mappings
        for datatype, mappings in self.general_mappings.items():
            self.general_mappings[datatype] = list(set(mappings))

        # extend the mappings into a dictionary with their nda targets
        for datatype, mappings in self.general_mappings.items():
            self.finished_product[datatype] = {
                m: re.sub("SUBJECT", "GUID", m) for m in mappings
            }

    def create_jsons(self):
        _file_names = []
        for datatype, mapper_json in self.finished_product.items():
            file_name = (
                Path(self.destination_path)
                / Path(self.nda_file_descriptor.format(datatype, datatype)
                + ".json")
            )
            with open(file_name, "w") as outfile:
                json.dump(mapper_json, outfile, indent=4)
            _file_names.append(file_name)
        return _file_names

    def create_yamls(self):
        templates = {
            "anat": {
                "image_description": "anatomical",
                "scan_type": "MR structural (T1)",
                "scan_object": "Live",
                "image_modality": "MRI",
                "transformation_performed": "No",
                "image_file_format": "DICOM",
            },
            "pet": {
                "image_description": "PET",
                "scan_type": "PET",
                "scan_object": "Live",
                "image_modality": "PET",
                "transformation_performed": "No",
                "image_file_format": "DICOM",
            },
        }

        _yaml_paths = []
        for datatype in self.datatypes:
            # build path to save yaml to
            yaml_path = (
                Path(self.destination_path)
                / Path(self.nda_file_descriptor.format(datatype, datatype)
                + ".yaml")
            )
            with open(yaml_path, "w") as outfile:
                yaml.dump(templates[datatype], outfile)
                _yaml_paths.append(yaml_path)
        return _yaml_paths

def cli():
    parser = ArgumentParser()
    parser.add_argument("bids_dataset", type=Path, help="BIDS directory to preprep for nda upload")
    parser.add_argument("destination_path", type=Path, help="Destination directory to place lookup.csv, file mapper jsons, and nda yaml files")
    args = parser.parse_args()
    MappingTemplator(bids_dataset=args.bids_dataset, destination_path=args.destination_path)

if __name__ == "__main__":
    cli()