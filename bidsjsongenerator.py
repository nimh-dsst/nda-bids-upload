import json
import bids
import pathlib
import argparse
import re


def collect_bids_mapping_templates(
    bids_dataset_path: pathlib.Path=None,
    collect_derivatives=True,
    filemapped_dataset_path= None
):
    if not bids_dataset_path.exists() or not bids_dataset_path:
        raise FileNotFoundError(bids_dataset_path)
    else:
        # expand path to absolute
        bids_dataset_path = bids_dataset_path.absolute()

    # load data with pybids
    dataset = bids.BIDSLayout(bids_dataset_path, derivatives=collect_derivatives)
    sessions = dataset.get_sessions()
    datatypes = dataset.get_datatypes()
    # create output path if none is given
    if not filemapped_dataset_path:
        filemapped_dataset_path = bids_dataset_path.parent / pathlib.Path(bids_dataset_path.name + '.nda')
    all_files = dataset.get_files()

    templates = set()

    for file_path in all_files:
        entities = dataset.get_file(file_path).entities
        template_entities = entities.copy()
        template_file_path = str(pathlib.Path(file_path).relative_to(bids_dataset_path))

        if "subject" in template_entities:
            template_file_path = re.sub(
                template_entities["subject"], "{SUBJECT}", template_file_path
            )
        if "session" in template_entities:
            template_file_path = re.sub(
                template_entities["session"], "{SESSION}", template_file_path
            )

        templates.add(template_file_path)

    organized_set = {}
    for datatype in datatypes:
        organized_set[datatype] = {
            "suffixes": dataset.get_suffixes(datatype=datatype),
            "templates": [],
        }
    for template in templates:
        # check to see what datatype a template belongs to
        for datatype, v in organized_set.items():
            is_datatype = any(suffix in template for suffix in v["suffixes"])
            if is_datatype:
                organized_set[datatype]["templates"].append(template)

    organized_set['bids_dataset'] = str(bids_dataset_path)
    organized_set['filemapped_dataset_path'] = str(filemapped_dataset_path)
    return organized_set


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bids_dateset_path", type=str)
    parser.add_argument("mapping_json", type=str)
    args = parser.parse_args()
    organized_set = collect_bids_mapping_templates(pathlib.Path(args.bids_dateset_path))
    print("\n" * 5)
    print("*" * 100)
    print(json.dumps(organized_set, indent=4))
    print("*" * 100)
