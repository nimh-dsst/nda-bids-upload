import bids
import pathlib
import os
import pandas
import json
from typing import Union
from argparse import ArgumentParser

class LookUpTable:
    def __init__(self, bids_dataset: str, destination_path=""):
        self.path_to_bids_dataset = str(bids_dataset)
        self.bids_layout = bids.BIDSLayout(self.path_to_bids_dataset)
        self.destination_path = destination_path
        self.participants_tsv = pandas.DataFrame()
        self.participants_json = None
        self.subject_list = self.bids_layout.get_subjects()
        self.subject_session_list = []
        self.lookup_table = pandas.DataFrame()

    def create_lookup_table(self):
        # check for a participants.tsv and .json
        if self.bids_layout.get(suffix="participants", extension="tsv"):
            try:
                self.participants_tsv = pandas.read_csv(
                    os.path.join(
                        self.path_to_bids_dataset,
                        self.bids_layout.get(suffix="participants", extension="tsv")[0],
                    ),
                    index_col="participant_id",
                    sep="\t",
                )
            except (FileExistsError, FileNotFoundError) as err:
                raise (err)
        if not self.participants_tsv.empty:
            participants_json_path = os.path.join(
                self.path_to_bids_dataset, "participants.json"
            )
            try:
                with open(participants_json_path, "r") as infile:
                    self.participants_json = json.load(infile)
            except (FileExistsError, FileNotFoundError, json.JSONDecodeError) as err:
                raise err

       
        age_multiplier = 1
        # check to see what units are in age
        if "y" in str.lower(self.participants_json.get("age", {}).get("Units", "")):
            age_multiplier = 12
        elif "m" in str.lower(self.participants_json.get("age", {}).get("Units", "")):
            age_multiplier = 1
        elif "w" in str.lower(self.participants_json.get("age", {}).get("Units", "")):
            age_multiplier = 1 / 4
        else:
            print(
                f"unable to determine participant.age.Units from "
                f"{self.bids_layout.get(suffix='participants', extension='json')}, "
                "not converting to months."
            )

        # check recast gender column and sidecar as sex if gender
        gender_col = next(
            (
                column
                for column in self.participants_tsv.columns
                if column.lower() == "gender"
            ),
            None,
        )
        if gender_col:
            self.participants_tsv = self.participants_tsv.rename(columns={gender_col: "sex"})
            self.participants_json["sex"] = self.participants_json.pop(gender_col)

        # create a subject/session list
        for s in self.subject_list:
            for entities in self.bids_layout.get(subject=s):
                ents = entities.get_entities()
                bids_subject_session = "sub-" + ents.get("subject")
                if ents.get("session"):
                    bids_subject_session += f'_ses-{ents.get("session")}'

                info = {
                    "bids_subject_session": bids_subject_session,
                    "subjectkey": "",
                    "src_subject_id": f"sub-{s}",
                    "interview_date": "",
                    "interview_age": age_multiplier
                    * float(self.participants_tsv["age"][f"sub-{s}"]),
                    "sex": self.participants_tsv["sex"][f"sub-{s}"],
                    "datatype": ents.get("datatype", ""),
                }
                self.subject_session_list.append(info)
        self.lookup_table = pandas.DataFrame(self.subject_session_list)

        return self.lookup_table

    def write_lookup_table(self):
        if self.lookup_table.empty:
            self.create_lookup_table()
        self.lookup_table.to_csv(
            self.destination_path, sep=",", na_rep="n/a", index=False
        )

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("bids_dataset", help="Path to bids dataset", type=pathlib.Path)
    parser.add_argument("destination_path", help="Path to nda upload folder", type=pathlib.Path)
    args = parser.parse_args()
    lookup_table = LookUpTable(str(args.bids_dataset), destination_path=str(args.destination_path))
    lookup_table.create_lookup_table()
    lookup_table.write_lookup_table()