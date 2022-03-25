import os
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag_path = os.getcwd()
# mapping columns for db and csv
columns_map = {
    "target": {"target_id": "id", "target_name": "name", "target_organism": "organism"},
    "molecule": {"molecule_id": "id", "molecule_name": "name", "molecule_max_phase": "max_phase",
                 "molecule_structure": "structure", "molecule_inchi_key": "inchi_key"},
    "activity": {'activity_id': "id", 'activity_type': "type", 'activity_units': "units",
                 'activity_value': "value", 'activity_relation': "relation", "molecule_id": "molecule_id",
                 "target_id": "target_id"}
}


def download_file_from_ftp():
    import shutil
    import urllib.request as request
    from contextlib import closing
    # download activities.csv into ftp_raw_data folder
    with closing(request.urlopen('https://ftp.ebi.ac.uk/pub/databases/chembl/other/activities.csv')) as r:
        with open(f'{dag_path}/ftp_raw_data/raw_activities.csv', 'wb') as f:
            shutil.copyfileobj(r, f)


def process_raw_data():
    import pandas as pd

    # read the raw data into a dataframe
    chunk_df = pd.read_csv(f"{dag_path}/ftp_raw_data/raw_activities.csv")

    # iterate through types of columns
    # should b
    for key, val in columns_map.items():
        df_list = chunk_df[list(val.keys())].drop_duplicates()
        df_list.rename(columns=val, inplace=True)
        df_list.to_csv(f'{dag_path}/processed_data/{key}.csv', index=False)


def load_data():
    import json
    import pandas as pd
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy import create_engine

    dburl = f"sqlite:///{dag_path}/db/ftp_processed.db"
    engine = create_engine(dburl)
    session = Session(bind=engine)
    base = automap_base()
    base.prepare(engine, reflect=True)

    #
    for key, val in columns_map.items():
        processed_df = pd.read_csv(f"{dag_path}/processed_data/{key}.csv", dtype={"id": int})
        map_list = json.loads(processed_df.to_json(orient='records'))
        target = base.classes.targets
        molecule = base.classes.molecules
        activity = base.classes.activities

        if key == "molecule":
            id_and_inchi = session.query(molecule.id, molecule.inchi_key).all()
            existing_id = [row[0] for row in id_and_inchi]
            existing_inchi = [row[1] for row in id_and_inchi]
            filtered = [val for val in map_list if
                        val["inchi_key"] not in existing_inchi and val["id"] not in existing_id]
            session.bulk_insert_mappings(molecule, filtered)

        if key == "target":
            existing_id = [row[0] for row in session.query(target.id).all()]
            filtered = set([val["id"] for val in map_list]) - set(existing_id)
            session.bulk_insert_mappings(target, filtered)

        if key == "activity":
            existing_id = [row[0] for row in session.query(activity.id).all()]
            filtered = set([val["id"] for val in map_list]) - set(existing_id)
            session.bulk_insert_mappings(activity, filtered)

    session.commit()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'ftp_csv_ingestion',
    default_args=default_args,
    description="Pull csv from ftp to sql db",
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='download_csv_from_ftp',
    python_callable=download_file_from_ftp,
    dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id='process_raw_csv',
    python_callable=process_raw_data,
    dag=ingestion_dag
)

task_3 = PythonOperator(
    task_id='processed_to_database',
    python_callable=load_data,
    dag=ingestion_dag
)

task_1 >> task_2 >> task_3
