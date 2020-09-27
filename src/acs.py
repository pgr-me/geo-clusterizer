
# standard library imports
import csv
import io
import os
from pathlib import Path
import sys
import zipfile
# third-party imports
import bs4
import requests
import urllib.request


class ACS:
    """ETL American Community Survey data.
    This code is mostly (~95%) based on the following gist
    https://gist.githubusercontent.com/erikbern/89c5f44bd1354854a8954fa2df04453d/raw/efd7b7c31d781a5cae9849be60ab86967bf7d2ed/american_community_survey_example.py
    Author of that code is Erik Bernhardsson | erikbern | https://gist.github.com/erikbern
    """
    def __init__(self, acs_year, acs_span, data_dir, overwrite=False):
        self.acs_year = acs_year
        self.acs_span = acs_span
        self.data_dir = Path(data_dir)
        self.lookup_url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{acs_year}/documentation/user_tools/ACS_{acs_span}yr_Seq_Table_Number_Lookup.txt'
        self.data_url = f'https://www2.census.gov/programs-surveys/acs/summary_file/{acs_year}/data/{acs_span}_year_by_state'
        self.lookup_path = self.data_dir / f'{acs_year}_{acs_span}y_lookup.txt'
        self.overwrite = overwrite
#         self.data_zips = []
    
    @staticmethod
    def download(src, dst, verbose=True):
        if verbose:
            print('downloading %s -> %s...' % (src, dst), file=sys.stderr)
        urllib.request.urlretrieve(src, dst)
        return True
    
    def get_acs_metadata(self):
        if not self.lookup_path.exists() and self.overwrite == False:
            self.download(self.lookup_url, self.lookup_path)
        return True
    
    def get_acs_data(self):
        # Go to the "data by state" page and scan the HTML page for links to zip files
        soup = bs4.BeautifulSoup(requests.get(self.data_url).content)
#         self.data_zips = []
        for link in soup.find_all('a'):
            if link.get('href') and link.get('href').endswith('zip'):
                fn = link.get('href').split('/')[-1]
                dst = self.data_dir / fn
                if not dst.exists() and self.overwrite == False:
                    self.download(self.data_url + '/' + fn, dst)
#                 self.data_zips.append(zipfile.ZipFile(os.path.join(self.data_dir, fn), 'r'))
        return True

    def find_table(self, table_title, subject_area):
        with open(self.lookup_path, 'r', encoding='iso-8859-1') as csvfile:
            reader = csv.DictReader(csvfile, dialect='unix')
            seq_number, start_pos, cells = None, None, []
            current_table_title = None
            for row in reader:
                if row['Table Title'] and row['Total Cells in Table']:
                    current_table_title = row['Table Title']
                if current_table_title == table_title and row['Start Position']:
                    seq_number = int(row['Sequence Number'])
                    start_pos = int(row['Start Position'])
                if current_table_title == table_title and row['Line Number']:
                    try:
                        int(row['Line Number'])
                        cells.append(row['Table Title'])
                    except:
                        pass

        return seq_number, start_pos, cells

    def get_geos(self):
        geos = {}
        for data_zip in self.data_zips:
            for info in data_zip.infolist():
                if info.filename.startswith('g') and info.filename.endswith('.csv'):
                    with data_zip.open(info.filename) as csvfile:
                        print('Parsing geography data for', info.filename, file=sys.stderr)
                        data = csvfile.read()
                        buf = io.StringIO(data.decode('iso-8859-1'))
                        reader = csv.reader(buf, dialect='unix')
                        for row in reader:
                            geos[(row[1], row[4])] = row[-4]
        return geos

    def get_table(self, table_title, subject_area):
        seq_number, start_pos, cells = self.find_table(table_title, subject_area)

        ret = {}
        for data_zip in self.data_zips:
            for info in data_zip.infolist():
                if info.filename.startswith('e') and info.filename.endswith('%04d000.txt' % seq_number):
                    with data_zip.open(info.filename) as csvfile:
                        print('Parsing data for', info.filename, file=sys.stderr)
                        data = csvfile.read()
                        buf = io.StringIO(data.decode('iso-8859-1'))
                        reader = csv.reader(buf, dialect='unix')
                        col_i, col_j = start_pos-1, start_pos+len(cells)-1
                        for row in reader:
                            state = row[2].upper()
                            logical_record_number = row[5]
                            values = [int(value) if (value and value != '.' and int(value) > 0) else None for value in row[col_i:col_j]]
                            ret[(state, logical_record_number)] = {k: v for k, v in zip(cells, values) if v is not None}
        return ret
