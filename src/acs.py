# standard library imports
import csv
import io
import os
from pathlib import Path
import sys
import urllib.request
import zipfile

# third-party imports
import bs4
import pandas as pd
import requests


class ACS:
    """ETL American Community Survey data.
    This code is mostly (~95%) based on the following gist
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    # TODO: #2 Refactor conditional so that they use pydoit's dependency management framework

    def __init__(
        self,
        acs_year,
        acs_span,
        raw_data_dir,
        interim_data_dir,
        lookup_src,
        overwrite=False,
        verbose=False,
    ):
        self.acs_year = acs_year
        self.acs_span = acs_span
        self.raw_data_dir = Path(raw_data_dir)
        self.interim_data_dir = Path(interim_data_dir)
        self.lookup_src = Path(
            lookup_src
        )  # this is a modfied version of what get_acs_metadata method downloads and saves; edit this file to specify which tables you want
        self.overwrite = overwrite
        self.verbose = verbose
        self.lookup_url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_year}/documentation/user_tools/ACS_{acs_span}yr_Seq_Table_Number_Lookup.txt"
        self.data_url = f"https://www2.census.gov/programs-surveys/acs/summary_file/{acs_year}/data/{acs_span}_year_by_state"
        self.lookup_path = (
            self.raw_data_dir / f"{acs_year}_{acs_span}y_lookup.txt"
        )  # this is the path to the unmodified lookups table, created by get_acs_metadata method, which we use to parse the acs data
        self.data_zips = []
        self.geos = pd.DataFrame()
        self.lookups = pd.DataFrame()
        self.acs_data = pd.DataFrame()
        self.acs_data_dst = self.interim_data_dir / "acs__tables.pkl"
        self.acs_preprocessed_data_dst = (
            self.interim_data_dir / "acs__preprocessed_tables.pkl"
        )

    @staticmethod
    def download(src, dst, verbose=False):
        if verbose:
            print("downloading %s -> %s..." % (src, dst), file=sys.stderr)
        urllib.request.urlretrieve(src, dst)
        return True

    def get_acs_metadata(self):
        # TODO: Refactor the conditional so that it uses pydoit's dependency management framework
        if not self.lookup_path.exists() and self.overwrite == False:
            self.download(self.lookup_url, self.lookup_path, verbose=self.verbose)
        return True

    def get_acs_data(self):
        # Go to the "data by state" page and scan the HTML page for links to zip files
        soup = bs4.BeautifulSoup(requests.get(self.data_url).content)
        for link in soup.find_all("a"):
            if link.get("href") and link.get("href").endswith("zip"):
                fn = link.get("href").split("/")[-1]
                dst = self.raw_data_dir / fn
                if not dst.exists() and self.overwrite == False:
                    self.download(self.data_url + "/" + fn, dst, verbose=self.verbose)
        return True

    def find_table(self, table_title, subject_area):
        with open(self.lookup_path, "r", encoding="iso-8859-1") as csvfile:
            reader = csv.DictReader(csvfile, dialect="unix")
            seq_number, start_pos, cells = None, None, []
            current_table_title = None
            for row in reader:
                if row["Table Title"] and row["Total Cells in Table"]:
                    current_table_title = row["Table Title"]
                if current_table_title == table_title and row["Start Position"]:
                    seq_number = int(row["Sequence Number"])
                    start_pos = int(row["Start Position"])
                if current_table_title == table_title and row["Line Number"]:
                    try:
                        int(row["Line Number"])
                        cells.append(row["Table Title"])
                    except:
                        pass
        return seq_number, start_pos, cells

    def get_data_zips(self):
        self.data_zips = [
            zipfile.ZipFile(x, "r")
            for x in self.raw_data_dir.iterdir()
            if x.suffix == ".zip"
        ]
        return True

    def get_geos(self):
        geos = {}
        for data_zip in self.data_zips:
            for info in data_zip.infolist():
                if info.filename.startswith("g") and info.filename.endswith(".csv"):
                    with data_zip.open(info.filename) as csvfile:
                        if self.verbose:
                            print(
                                "Parsing geography data for",
                                info.filename,
                                file=sys.stderr,
                            )
                        data = csvfile.read()
                        buf = io.StringIO(data.decode("iso-8859-1"))
                        reader = csv.reader(buf, dialect="unix")
                        for row in reader:
                            geos[(row[1], row[4])] = {
                                "geo_label": row[-4],
                                "geoid": row[-5].split("US")[-1],
                            }
        self.geos = pd.DataFrame.from_dict(geos, orient="index")
        self.geos.reset_index(inplace=True)
        self.geos.rename(
            columns={"level_0": "state_abbr", "level_1": "logrecno"}, inplace=True
        )
        m = self.geos["geoid"].apply(lambda x: len(x)) == 11
        self.geos = self.geos[m]
        self.geos["state_abbr__logrecno"] = (
            self.geos["state_abbr"] + "__" + self.geos["logrecno"]
        )
        self.geos.set_index("state_abbr__logrecno", inplace=True)

        return True

    def get_lookups(self):
        self.lookups = pd.read_csv(self.lookup_src, comment="#")
        self.lookups.columns = [
            x.lower().strip().replace(" ", "_") for x in self.lookups
        ]
        self.lookups = self.lookups.query('(get==1) | (get=="1")').fillna(
            method="ffill"
        )
        lookups_subject_map = {
            "Age-Sex": "age_sex",
            "Ancestry": "ancestry",
            "Children - Relationship": "children_relationship",
            "Disability": "disability",
            "Earnings": "earnings",
            "Educational Attainment": "edu_attain",
            "Employment Status": "emp_status",
            "Fertility": "fertility",
            "Foreign Birth": "foreign_birth",
            "Grand(Persons) - Age of HH Members": "grandparents",
            "Group Quarters": "gq",
            "Health Insurance": "health_insurance",
            "Hispanic Origin": "hisp_orig",
            "Households - Families": "hh_fams",
            "Housing": "housing",
            "Income": "inc",
            "Industry-Occupation-Class of Worker": "ind_occup",
            "Journey to Work": "commute",
            "Language": "lang",
            "Marital Status": "marital_status",
            "Place of Birth - Native": "native_birthplace",
            "Poverty": "poverty",
            "Race": "race",
            "Residence Last Year - Migration": "res_last_year_mig",
            "School Enrollment": "school_enroll",
            "Transfer Programs": "transfer_progs",
            "Unweighted Count": "unweighted_ct",
            "Veteran Status": "vet_status",
        }
        self.lookups["subject_abbr"] = self.lookups["subject_area"].replace(
            lookups_subject_map
        )
        return True

    def parse_table(self, table_title, subject_area, subject_abbr):
        if (len(self.geos) == 0) or (len(self.lookups) == 0):
            raise ValueError(
                "Must run get_geos AND get_lookups methods before running parse_table method"
            )
        seq_number, start_pos, cells = self.find_table(table_title, subject_area)
        table = {}
        for data_zip in self.data_zips:
            for info in data_zip.infolist():
                if info.filename.startswith("e") and info.filename.endswith(
                    "%04d000.txt" % seq_number
                ):
                    with data_zip.open(info.filename) as csvfile:
                        if self.verbose:
                            print("Parsing data for", info.filename, file=sys.stderr)
                        data = csvfile.read()
                        buf = io.StringIO(data.decode("iso-8859-1"))
                        reader = csv.reader(buf, dialect="unix")
                        col_i, col_j = start_pos - 1, start_pos + len(cells) - 1
                        for row in reader:
                            state = row[2].upper()
                            logical_record_number = row[5]
                            try:
                                values = [
                                    int(value)
                                    if (value and value != "." and int(value) > 0)
                                    else None
                                    for value in row[col_i:col_j]
                                ]
                            except ValueError:
                                values = [
                                    float(value)
                                    if (value and value != "." and float(value) >= 0)
                                    else None
                                    for value in row[col_i:col_j]
                                ]
                            key = f"{state}__{logical_record_number}"
                            table[key] = {
                                k: v for k, v in zip(cells, values) if v is not None
                            }
        table = pd.DataFrame.from_dict(table).transpose()
        table_id = (
            self.lookups.query(
                "(table_title==@table_title) & (subject_area==@subject_area)"
            )
            .iloc[0]
            .loc["table_id"]
        )
        table.columns = [x.replace(":", "").strip().replace(" ", "_") for x in table]
        table.columns = [f"{subject_abbr}__{table_id}__{x}".lower() for x in table]
        table = (
            table.join(self.geos)
            .set_index(["state_abbr", "logrecno", "geo_label", "geoid"])
            .reset_index()
            .set_index("geoid")
            .dropna()
            .sort_index()
        )
        return table

    def parse_tables(self):
        for row in self.lookups.iterrows():
            table_id, table_title, subject_area, subject_abbr = (
                row[1].loc["table_id"],
                row[1].loc["table_title"],
                row[1].loc["subject_area"],
                row[1].loc["subject_abbr"],
            )
            try:
                dst = self.interim_data_dir / f"acs__table_{table_id}.pkl"
                # TODO: Refactor the conditional so that it uses pydoit's dependency management framework
                if (not dst.exists()) or (self.overwrite):
                    if self.verbose:
                        print("*", end="")
                    self.parse_table(table_title, subject_area, subject_abbr).to_pickle(
                        dst
                    )
            except:
                # TODO: re-write this try except block to handle the specific TypeError that was raised in parse_table method
                pass
        return True

    def join_tables(self):
        # TODO: Refactor the conditional so that it uses pydoit's dependency management framework
        if self.acs_data_dst.exists() and (not self.overwrite):
            self.acs_data = pd.read_pickle(self.acs_data_dst)
        else:
            self.acs_data = (
                pd.Series(self.geos.geoid.unique(), name="geoid")
                .to_frame()
                .set_index("geoid")
                .sort_index()
            )
            for path in self.interim_data_dir.iterdir():
                if path.stem.startswith("acs__table"):
                    table = pd.read_pickle(path)
                    c = [x for x in table if x not in self.acs_data]
                    if len(c) > 0:
                        self.acs_data = self.acs_data.join(table[c], how="left")
            self.acs_data.to_pickle(self.acs_data_dst)

        return True

    def preprocess_tables(self, null_thresh=4000, states_only=True):
        # TODO: Refactor the conditional so that it uses pydoit's dependency management framework
        if self.acs_preprocessed_data_dst.exists() and (not self.overwrite):
            self.preprocessed_acs_data = pd.read_pickle(self.acs_preprocessed_data_dst)
        else:
            m = self.acs_data.isnull().sum() < null_thresh
            self.preprocessed_acs_data = self.acs_data.copy()[m[m].index.values]
            if states_only:
                self.preprocessed_acs_data = self.preprocessed_acs_data.query(
                    "state_abbr.notnull()"
                )
            self.preprocessed_acs_data.columns = [
                x.replace(",", "").replace("/", "").replace(".", "").replace(":", "")
                for x in self.preprocessed_acs_data
            ]
            self.preprocessed_acs_data.to_pickle(self.acs_preprocessed_data_dst)
        return True
