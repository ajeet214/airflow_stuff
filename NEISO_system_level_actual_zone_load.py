"""
# Innowatts Airflow Client Pipeline
### Rep Code: NEISO
### System Level Actual Data Load
### Expected Run Time - daily
### Expected Arrival Time - daily
### Author: Ajeet Verma
"""

# External and Base Modules
import pytz
from datetime import datetime, timedelta, timezone

# Airflow Modules
from airflow.models import Variable
from airflow import DAG, macros
from airflow.operators import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Internal Modules
from innowatts_airflow_core.model.airflow_env_config import Airflow_Env_Config
from innowatts_airflow_core.utils.slack import slack_on_failure

from common.scripts.getdata.NEISO_system_level_actual_zone_load import NeisoSystemLevelLoad

env = Variable.get("env_prefix")

airflow_env = Airflow_Env_Config(env, ["kyle@innowatts.com"], True, '0 8   *')

start_date = datetime(2019, 12, 25, 0, 0)

default_args = {
    "owner": "Ajeet",
    "depends_on_past": False,
    "start_date": start_date,
    "email": airflow_env.email,
    "email_on_failure": airflow_env.email_on_failure,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=6),
}

dag = DAG(
    "NEISO_system_level_actual_zone_load",
    catchup=airflow_env.catchup,
    schedule_interval=airflow_env.schedule_interval,
    default_args=default_args,
    max_active_runs=1,
)
dag.doc_md = _doc_

# Task Tuning
default_execution_timeout = timedelta(seconds=45)
default_retry_delay = timedelta(seconds=10)
default_retries = 10
default_retry_exponential_backoff = True
poke_timeout = 1800
poke_interval = 30

# Task Specified
db_details = Variable.get('lepus_mysql', deserialize_json=True)
database = "master_data_db"
api_cred = Variable.get('neiso_system_level_actual_api_cred', deserialize_json=True)

obj = NeisoSystemLevelLoad(db_details['user'], db_details['password'], db_details['host'], 3306, database)

start_dag = DummyOperator(
    task_id='start_neiso_data_load',
    dag=dag,
)
end_dag = DummyOperator(
    task_id='end_neiso_data_load',
    dag=dag,
)

tz = pytz.timezone('America/Chicago')
day = datetime.now(tz)-timedelta(days=4)

fetch_neiso_system_level_actual_zone_load = PythonOperator(
    dag=dag,
    task_id='Load_neiso_system_level_actual_zone_load_to_mysql',
    provide_context=False,
    python_callable=obj.system_level_load_neiso,
    op_kwargs={'day': int(day.strftime('%Y%m%d')),
               'username': api_cred['username'],
               'password': api_cred['password'],
               })


start_dag >> fetch_neiso_system_level_actual_zone_load >> end_dag
