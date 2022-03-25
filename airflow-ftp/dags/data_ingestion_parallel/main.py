import os
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

dag_path = os.getcwd()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}


def download_file_from_ftp(url):
    import shutil
    import urllib.request as request
    from contextlib import closing
    # download activities.csv into ftp_raw_data folder
    with closing(request.urlopen(f'{url}')) as r:
        with open(f'{dag_path}/ftp_raw_data/raw_activities.csv', 'wb') as f:
            shutil.copyfileobj(r, f)


def process_raw_data(name, entity_columns):
    import pandas as pd

    # read the raw data into a dataframe
    chunk_df = pd.read_csv(f"{dag_path}/ftp_raw_data/raw_activities.csv")

    # iterate through types of columns
    # should b
    df_list = chunk_df[list(entity_columns.keys())].drop_duplicates()
    df_list.rename(columns=entity_columns, inplace=True)
    df_list.to_csv(f'{dag_path}/processed_data/{name}.csv', index=False)


def load_data(name):
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
    processed_df = pd.read_csv(f"{dag_path}/processed_data/{name}.csv", dtype={"id": int})
    map_list = json.loads(processed_df.to_json(orient='records'))
    target = base.classes.targets
    molecule = base.classes.molecules
    activity = base.classes.activities

    # insert molecules
    if name == "molecule":
        molecule_id_and_inchi = session.query(molecule.id, molecule.inchi_key).all()
        existing_molecule_id = [row[0] for row in molecule_id_and_inchi]
        existing_inchi = [row[1] for row in molecule_id_and_inchi]
        molecule_filtered = [val for val in map_list if
                             val["inchi_key"] not in existing_inchi and val["id"] not in existing_molecule_id]
        session.bulk_insert_mappings(molecule, molecule_filtered)

    # insert processed target data
    if name == "target":
        existing_target_id = [row[0] for row in session.query(target.id).all()]
        target_filtered = set([val["id"] for val in map_list]) - set(existing_target_id)
        session.bulk_insert_mappings(target, target_filtered)

    # insert processed activities with foreign keys from molecules and target
    if name == "activity":
        existing_activity_id = [row[0] for row in session.query(activity.id).all()]
        activity_filtered = set([val["id"] for val in map_list]) - set(existing_activity_id)
        session.bulk_insert_mappings(activity, activity_filtered)

    session.commit()


with DAG(
        'ftp_csv_ingestion_parallel',
        default_args=default_args,
        description="Pull csv from ftp to sql db",
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:
    download_csv_from_ftp = PythonOperator(
        task_id='download_csv_from_ftp',
        python_callable=download_file_from_ftp,
        op_kwargs={'url': 'https://ftp.ebi.ac.uk/pub/databases/chembl/other/activities.csv'}
    )

    process_raw_target_data = PythonOperator(
        task_id='process_raw_target_data',
        python_callable=process_raw_data,
        op_kwargs={
            'name': 'target',
            'entity_columns': {"target_id": "id", "target_name": "name", "target_organism": "organism"}
        }
    )

    process_raw_molecule_data = PythonOperator(
        task_id='process_raw_molecule_data',
        python_callable=process_raw_data,
        op_kwargs={
            'name': 'molecule',
            'entity_columns': {"molecule_id": "id", "molecule_name": "name", "molecule_max_phase": "max_phase",
                               "molecule_structure": "structure", "molecule_inchi_key": "inchi_key"}
        }
    )

    process_raw_activity_data = PythonOperator(
        task_id='process_raw_activity_data',
        python_callable=process_raw_data,
        op_kwargs={
            'name': 'activity',
            'entity_columns': {'activity_id': "id", 'activity_type': "type", 'activity_units': "units",
                               'activity_value': "value", 'activity_relation': "relation", "molecule_id": "molecule_id",
                               "target_id": "target_id"}
        }
    )

    finished_processing = BashOperator(
        task_id="finished_processing",
        bash_command="date"
    )

    wait_to_complete_processing = ExternalTaskSensor(
        task_id='wait_to_complete_processing',
        external_dag_id=dag.dag_id,
        external_task_id='finished_processing')

    insert_target_processed_data = PythonOperator(
        task_id='insert_target_processed_data',
        python_callable=load_data,
        op_kwargs={
            'name': 'target'
        }
    )

    insert_molecule_processed_data = PythonOperator(
        task_id='insert_molecule_processed_data',
        python_callable=load_data,
        op_kwargs={
            'name': 'molecule'
        }
    )

    insert_activity_processed_data = PythonOperator(
        task_id='insert_activity_processed_data',
        python_callable=load_data,
        op_kwargs={
            'name': 'activity'
        }
    )
    
    download_csv_from_ftp >> [process_raw_target_data, process_raw_molecule_data,
                              process_raw_activity_data] >> finished_processing

    finished_processing >> wait_to_complete_processing >> [
        insert_target_processed_data, insert_molecule_processed_data, insert_activity_processed_data]
