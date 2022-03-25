import contextlib
import json
import logging
import time
import datetime
import urllib.request as request
import pandas as pd
from functools import wraps

from Connection import Connection

logging.basicConfig(filename="activity_logs.log",
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger()


def my_logs(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        print(f'Process Started at: {datetime.datetime.now()}')
        logger.info(f'Process Started at: {datetime.datetime.now()}')

        result = func(*args, **kwargs)

        end_time = time.perf_counter()
        print(f'Process Ended at: {datetime.datetime.now()}')
        logger.info(f'Process Ended at: {datetime.datetime.now()}')

        total_time = end_time - start_time
        print(f'Time taken for Upload: {total_time:.4f} seconds')
        logger.info(f'Time taken for Upload: {total_time:.4f} seconds')
        logger.info(f'==============================================')
        return result

    return timeit_wrapper


class Fetch:
    def __init__(self):
        self.conn = Connection()
        self.conn.setup_database("sqlite:///ftp.db", False)
        self.file_name = "https://ftp.ebi.ac.uk/pub/databases/chembl/other/activities.csv"
        self.chunk = 500
        self.conn.fetch_count()
        self.column_names = {
            "target": {"target_id": "id", "target_name": "name", "target_organism": "organism"},
            "molecule": {"molecule_id": "id", "molecule_name": "name", "molecule_max_phase": "max_phase",
                         "molecule_structure": "structure", "molecule_inchi_key": "inchi_key"},
            "activity": {'activity_id': "id", 'activity_type': "type", 'activity_units': "units",
                         'activity_value': "value", 'activity_relation': "relation", "molecule_id": "molecule_id",
                         "target_id": "target_id"}
        }

    def part_to_csv(self, chunk_df):
        for key, val in self.column_names.items():
            df_list = chunk_df[list(val.keys())].drop_duplicates()
            df_list.rename(columns=val, inplace=True)
            df_list.to_csv(f'data/{key}.csv', mode='a', index=False, header=False)

    @my_logs
    def bulk_insert(self, chunk_df):
        for key, val in self.column_names.items():
            df_list = chunk_df[list(val.keys())].drop_duplicates()
            df_list.rename(columns=val, inplace=True)
            map_list = json.loads(df_list.to_json(orient='records'))
            self.conn.test_bulk_insert_mappings(key, map_list)

    @my_logs
    def df_bulk_insert(self):
        self.conn.df_to_db(list(self.column_names.keys()))

    @my_logs
    def read_by_chunk(self):
        with contextlib.closing(request.urlopen(url=self.file_name)) as rd:
            for df in pd.read_csv(rd, chunksize=self.chunk):
                # self.bulk_insert(df)
                self.part_to_csv(df)

        self.report()

    def report(self):
        report = self.conn.fetch_count()
        for key, val in report.items():
            print(f'Count of {key}: {val}')
            logger.info(f'Count of {key}: {val}')


if __name__ == '__main__':
    ftp_fetch = Fetch()
    ftp_fetch.read_by_chunk()
    ftp_fetch.df_bulk_insert()
