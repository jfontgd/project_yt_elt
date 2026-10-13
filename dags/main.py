from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (get_playlist_id,  
get_video_ids, extract_video_data, save_to_json)

from datawarehouse.dwh import staging_table, core_table
from data_quality.soda import yt_elt_data_quality

# Define local tz
local_tz = pendulum.timezone('Asia/Manila')

#Default Args for the DAG
default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 6, tzinfo=local_tz),
    'email': ['data@engineers.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

#Variables
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id = 'produce_json',
    default_args=default_args,
    description = 'Dag to produce JSON file with the raw data',
    schedule =' 0 14 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # Define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task

with DAG(
    dag_id = 'update_db',
    default_args=default_args,
    description = 'Dag to process json file and insert data into both staging and core schemas',
    schedule =' 0 15 * * *',
    catchup = False
) as dag:
    
    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core

    with DAG(
    dag_id = 'yt_elt_data_quality',
    default_args=default_args,
    description = 'Dag to check data quality of the data in the staging and core schemas',
    schedule =' 0 16 * * *',
    catchup = False
) as dag:
    
    # Define tasks
        soda_validate_staging = yt_elt_data_quality(staging_schema)
        soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
        soda_validate_staging >> soda_validate_core
    
