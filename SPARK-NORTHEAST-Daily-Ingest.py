"""
# Innowatts Airflow Client Pipeline
### Rep Code: Spark NORTHEAST
### Expected Arrival Time - daily

### Configuration
* aws_conn_id = 's3-development'
* emr_conn_id = 'emr_default'
* region_name = 'us-east-2'
* rep_code = '{}_sparkenergy'.format(env)
* s3_ftp_bucket = '{}-innowatts-sparkenergy-ftp'.format(env)
* s3_processing_bucket = '{}-innowatts-sparkenergy-processing'.format(env)
* staging_config = Variable.get("staging_config")
* client_market = 'northeast'
* hive_incoming_table_name = 'r_northeast_idr_interval_usage'
* hive_outgoing_table_name = 'p_northeast_idr_interval_usage'
* unprocessed_s3_staging_full = "s3://internal-innowatts-airflow-staging/SPARK/{}/NORTHEAST/daily/".format(env)
* slack_key = Variable.get('slack_api_token')

### Steps:
* find_customer_info_file >> does_customer_info_file_exist >> meter_info_found >> t_meter_info_remove_extra_text >> t_move_customer_info >> t_add_partition_customer_info >> t_verify_partition_customer_info >> finish_ops
* does_customer_info_file_exist >> meter_info_missing >> customer_info_email_unavailable_file >> finish_ops

* find_meter_info_file >> does_meter_info_file_exist >> meter_attributes_found >> t_meter_attributes_remove_extra_text >> t_move_meter_info >> t_add_partition_meter_info >> t_verify_partition_meter_info >> finish_ops
* does_meter_info_file_exist >> meter_attributes_missing >> meter_info_email_unavailable_file >> finish_ops

* find_scalar_file >> does_scalar_file_exist >> scalar_found >> t_scalar_remove_extra_text >> t_move_scalar_usage >> t_add_partition_scalar_usage >> t_verify_partition_scalar_usage >> finish_ops
* does_scalar_file_exist >> scalar_missing >> scalar_email_unavailable_file >> finish_ops

This DAG retrieves Customer Info, Meter Info, and Scalar data from the Spark ftp for the NORTHEAST market.
Raw files are produced for all.

### Author: Ajeet Verma
"""

# External and Base Modules
import boto3
from datetime import datetime, timedelta

# Airflow Modules
import airflow.macros
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from common.operators.emr_add_steps_operator import EmrAddStepsOperatorMod
from common.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperatorMod
from common.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperatorMod
from common.sensors.emr_step_sensor import EmrStepSensorMod

from innowatts_airflow_core.operators.zip_operator_plugin import UnzipOperator
from airflow.operators.innowattsairflowcore import S3ToS3Operator
from airflow.operators.innowattsairflowcore import AwsAthenaOperator
from airflow.operators.innowattsairflowcore import AthenaQuerySensor


# Internal Modules
from common.utils.slack import slack_on_failure
from common.utils.slack import slack_on_success
from innowatts_airflow_core.model.airflow_env_config import Airflow_Env_Config
from airflow.utils.trigger_rule import TriggerRule

env = Variable.get('env_prefix')
airflow_env = Airflow_Env_Config(env, ["ajeet.verma@innowatts.com"], True, '0 10   *') #at 23:00PM UTC
code_prefix = Variable.get('code_prefix')

start_date = datetime(2020, 1, 20, 0, 0)

default_args = {
  'owner': 'Ajeet',
  'depends_on_past': False,
  'start_date': start_date,
  'email': airflow_env.email,
  'email_on_failure': airflow_env.email_on_failure,
  'email_on_retry': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=30)
}

dag = DAG('SPARK-NORTHEAST-Daily-Ingest',
          catchup=airflow_env.catchup,
          schedule_interval=airflow_env.schedule_interval,
          concurrency=6,
          max_active_runs=1,
          default_args=default_args)
dag.doc_md = _doc_

# Task Tuning
default_execution_timeout = timedelta(seconds=45)
default_retry_delay = timedelta(seconds=10)
default_retries = 10
default_retry_exponential_backoff = True
poke_timeout = 1800
poke_interval = 30

# Task Specific
aws_conn_id = 's3-development'
emr_conn_id = 'emr_default'
region_name = 'us-east-2'

# Ingest Specific
rep_code = '{}_sparkenergy'.format(env)
s3_ftp_bucket = '{}-innowatts-sparkenergy-ftp'.format(env)
s3_processing_bucket = '{}-innowatts-sparkenergy-processing'.format(env)
staging_config = Variable.get("staging_config")
client_market = 'northeast'
unprocessed_s3_staging_full = "s3://internal-innowatts-airflow-staging/SPARK/{}/NORTHEAST/daily/".format(env)
slack_key = Variable.get('slack_api_token')
email_group = Variable.get("sparkenergy_email_alerts")  #"support@innowatts.com"
local_s3mount = "/home/ubuntu/s3buckets/"
local_s3_ftp = "sparkenergy-ftp/FromSpark_Gas/"
s3_ftp = r"sparkenergy-ftp/FromSpark_Gas/"
s3_local_processing_bucket = "sparkenergy-processing"

file_date_format = (
    "{{ macros.ds_format( macros.ds_add( ds, 1 ) , '%Y-%m-%d', '%Y%m%d') }}"
)

today_process_date = (
    "{{ macros.ds_add( ds, 1 ) }}"
)

file_date_format = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%Y%m%d") }}'

file_yesterday_format = '{{ macros.ds_format((ds), "%Y-%m-%d", "%Y%m%d") }}'
raw_source_yesterday_customer_info = 'FromSpark_Gas/CUSTOMER_{}_NORTHEAST'.format(file_yesterday_format)
raw_source_yesterday_meter_info = 'FromSpark_Gas/METER_{}_NORTHEAST'.format(file_yesterday_format)
raw_source_yesterday_scalar_usage = 'FromSpark_Gas/SCALAR_USAGE_{}_NORTHEAST'.format(file_yesterday_format)

raw_source_customer_info = 'FromSpark_Gas/CUSTOMER_{}_NORTHEAST'.format(file_date_format)
raw_source_meter_info = 'FromSpark_Gas/METER_{}_NORTHEAST'.format(file_date_format)
raw_source_scalar_usage = 'FromSpark_Gas/SCALAR_USAGE_{}_NORTHEAST'.format(file_date_format)

raw_dest_customer_info = 'gas/northeast/{}/process_date={}/'.format('r_northeast_customer_info', today_process_date)
raw_dest_meter_info = 'gas/northeast/{}/process_date={}/'.format('r_northeast_meter_info', today_process_date)
raw_dest_scalar_usage = 'gas/northeast/{}/process_date={}/'.format('r_northeast_scalar_usage', today_process_date)
# proc_dest_scalar_usage = 'northeast/usage/{}/process_date={}/'.format('p_pjm_scalar_usage',today_process_date)


def customer_info_file_exists(**context):
    task_string = context["templates_dict"]["task_string"]
    # logging.info("This is the return value from the find_rt_file task: {}:".format(task_string))
    if task_string[:9] == "NOT FOUND":
        branch = "MISSING_CUSTOMER_INFO"
    else:
        branch = "FOUND_CUSTOMER_INFO"
    return branch


def meter_info_file_exists(**context):
    task_string = context["templates_dict"]["task_string"]
    # logging.info("This is the return value from the find_rt_file task: {}:".format(task_string))
    if task_string[:9] == "NOT FOUND":
        branch = "MISSING_METER_INFO"
    else:
        branch = "FOUND_METER_INFO"
    return branch


def scalar_usage_file_exists(**context):
    task_string = context["templates_dict"]["task_string"]
    # logging.info("This is the return value from the find_rt_file task: {}:".format(task_string))
    if task_string[:9] == "NOT FOUND":
        branch = "MISSING_SCALAR"
    else:
        branch = "FOUND_SCALAR"
    return branch


meter_info_missing = DummyOperator(task_id="MISSING_CUSTOMER_INFO", dag=dag, on_success_callback=slack_on_success)
meter_info_found = DummyOperator(task_id="FOUND_CUSTOMER_INFO", dag=dag)
meter_attributes_missing = DummyOperator(task_id="MISSING_METER_INFO", dag=dag, on_success_callback=slack_on_success)
meter_attributes_found = DummyOperator(task_id="FOUND_METER_INFO", dag=dag)
scalar_missing = DummyOperator(task_id="MISSING_SCALAR", dag=dag, on_success_callback=slack_on_success)
scalar_found = DummyOperator(task_id="FOUND_SCALAR", dag=dag)
finish_ops = DummyOperator(task_id="DONE", dag=dag)
start_ops = DummyOperator(task_id="Start", dag=dag)
meter_unzip = DummyOperator(task_id="customer_unzip", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
attribute_unzip = DummyOperator(task_id="meter_unzip", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)
scalar_unzip = DummyOperator(task_id="scalar_unzip", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

# Customer Info
find_customer_info_file = BashOperator(
    bash_command="""file_name=$(find {mount}{ftp} -maxdepth 1 -name "CUSTOMER_{file_date}_NORTHEAST*" -printf '%T+ %p\n' | sort -r | head -1);
                    if [[ ! -z "$file_name" ]]; then
                        base_file=$(basename -- "$file_name");
                        echo "$base_file"
                    else
                        echo "NOT FOUND $base_file"
                    fi
    """.format(
        mount=local_s3mount, ftp=local_s3_ftp, file_date=file_date_format
    ),
    xcom_push=True,
    # Base Params
    task_id="find_customer_info_file",
    priority_weight=10,
    depends_on_past=False,
    pool="default",
    queue="default",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

does_customer_info_file_exist = BranchPythonOperator(
    provide_context=True,
    python_callable=customer_info_file_exists,
    templates_dict={
        "task_string": "{{ task_instance.xcom_pull(task_ids='find_customer_info_file') }}"
    },
    # Base Operators
    task_id="does_customer_info_file_exist",
    trigger_rule="all_done",
    dag=dag,
)

customer_info_email_unavailable_file = EmailOperator(
    task_id="customer_info_email_unavailable_file",
    to=["{}".format(email_group)],
    subject="Spark Energy Task Failure: File Not Found CUSTOMER_{file_date}*".format(
        file_date=file_date_format
    ),
    html_content=""" <p><h3>Failure Report: SPARK File Not Found for {file_date}.
    <br /> Action Taken: Re-using yesterday's file.<br />
        <br /> This is an automated email. </h3>
        <br /> From: support@innowatts.com</p>
        """.format(
        file_date=file_date_format
    ),
    # Base
    dag=dag,
)


t_unzip_customer_info = UnzipOperator(
    task_id='unzip_customer_info',
    path_to_zip_file=local_s3mount + s3_ftp + 'CUSTOMER_{}_NORTHEAST.zip'.format(file_date_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)


t_unzip_yesterdays_customer_info = UnzipOperator(
    task_id='unzip_yesterday_customer_info',
    path_to_zip_file=local_s3mount + s3_ftp + 'CUSTOMER_{}_NORTHEAST.zip'.format(file_yesterday_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)


t_move_yesterday_customer_info = S3ToS3Operator(
    task_id="move_yesterday_customer_info",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_yesterday_customer_info),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path=r'{}.csv'.format(raw_source_customer_info),
    s3_destination_bucket=s3_ftp_bucket,
    aws_conn_id=aws_conn_id,
    mode="move",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_move_customer_info = S3ToS3Operator(
    task_id="move_customer_info",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_customer_info),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path='{}CUSTOMER_{}_NORTHEAST.csv'.format(raw_dest_customer_info, file_date_format),
    s3_destination_bucket=s3_processing_bucket,
    aws_conn_id=aws_conn_id,
    mode="move",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_add_partition_customer_info = AwsAthenaOperator(
    query=(
        "ALTER TABLE {}.r_northeast_customer_info ADD PARTITION".format(
            rep_code
        )
        + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
        + ' location "s3://{}/{}"'.format(s3_processing_bucket, raw_dest_customer_info)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_customer_info",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_verify_partition_customer_info = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_customer_info') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_customer_info",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

# Meter Info
find_meter_info_file = BashOperator(
    bash_command="""file_name=$(find {mount}{ftp} -maxdepth 1 -name "METER_{file_date}_NORTHEAST*" -printf '%T+ %p\n' | sort -r | head -1);
                    if [[ ! -z "$file_name" ]]; then
                        base_file=$(basename -- "$file_name");
                        echo "$base_file"
                    else
                        echo "NOT FOUND $base_file"
                    fi
    """.format(
        mount=local_s3mount, ftp=local_s3_ftp, file_date=file_date_format
    ),
    xcom_push=True,
    # Base Params
    task_id="find_meter_info_file",
    priority_weight=10,
    depends_on_past=False,
    pool="default",
    queue="default",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

does_meter_info_file_exist = BranchPythonOperator(
    provide_context=True,
    python_callable=meter_info_file_exists,
    templates_dict={
        "task_string": "{{ task_instance.xcom_pull(task_ids='find_meter_info_file') }}"
    },
    # Base Operators
    task_id="does_meter_info_file_exist",
    trigger_rule="all_done",
    dag=dag,
)

meter_info_email_unavailable_file = EmailOperator(
    task_id="meter_info_email_unavailable_file",
    to=["{}".format(email_group)],
    subject="SPARK Task Failure: File Not Found METER_{file_date}*".format(
        file_date=file_date_format
    ),
    html_content=""" <p><h3>Failure Report: SPARK File Not Found for {file_date}.
    <br /> Action Taken: Re-using yesterday's file.<br />
        <br /> This is an automated email. </h3>
        <br /> From: support@innowatts.com</p>
        """.format(
        file_date=file_date_format
    ),
    # Base
    dag=dag,
)

t_unzip_meter_info = UnzipOperator(
    task_id='unzip_meter_info',
    path_to_zip_file=local_s3mount + s3_ftp + 'METER_{}_NORTHEAST.zip'.format(file_date_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)

t_unzip_yesterday_meter_info = UnzipOperator(
    task_id='unzip_yesterday_meter_info',
    path_to_zip_file=local_s3mount + s3_ftp + 'METER_{}_NORTHEAST.zip'.format(file_yesterday_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)


t_move_yesterday_meter_info = S3ToS3Operator(
    task_id="move_yesterday_meter_info",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_yesterday_meter_info),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path=r'{}.csv'.format(raw_source_meter_info),
    s3_destination_bucket=s3_ftp_bucket,
    aws_conn_id=aws_conn_id,
    mode="move",
    dag=dag,
    on_failure_callback=slack_on_failure,
)


t_move_meter_info = S3ToS3Operator(
    task_id="move_meter_info",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_meter_info),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path='{}METER_{}_NORTHEAST.csv'.format(raw_dest_meter_info, file_date_format),
    s3_destination_bucket=s3_processing_bucket,
    aws_conn_id=aws_conn_id,
    mode="move",
    dag=dag,
    on_failure_callback=slack_on_failure,
)


t_add_partition_meter_info = AwsAthenaOperator(
    query=(
        "ALTER TABLE {}.r_northeast_meter_info ADD PARTITION".format(
            rep_code
        )
        + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
        + ' location "s3://{}/{}"'.format(s3_processing_bucket, raw_dest_meter_info)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_meter_info",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_verify_partition_meter_info = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_meter_info') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_meter_info",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

# Scalar
find_scalar_file = BashOperator(
    bash_command="""file_name=$(find {mount}{ftp} -maxdepth 1 -name "SCALAR_USAGE_{file_date}_NORTHEAST*" -printf '%T+ %p\n' | sort -r | head -1);
                    if [[ ! -z "$file_name" ]]; then
                        base_file=$(basename -- "$file_name");
                        echo "$base_file"
                    else
                        echo "NOT FOUND $base_file"
                    fi
    """.format(
        mount=local_s3mount, ftp=local_s3_ftp, file_date=file_date_format
    ),
    xcom_push=True,
    #Base Params
    task_id="find_scalar_file",
    priority_weight=10,
    depends_on_past=False,
    pool="default",
    queue="default",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

does_scalar_file_exist = BranchPythonOperator(
    provide_context=True,
    python_callable=scalar_usage_file_exists,
    templates_dict={
        "task_string": "{{ task_instance.xcom_pull(task_ids='find_scalar_file') }}"
    },
    # Base Operators
    task_id="does_scalar_file_exist",
    trigger_rule="all_done",
    dag=dag,
)

scalar_email_unavailable_file = EmailOperator(
    task_id="scalar_email_unavailable_file",
    to=["{}".format(email_group)],
    subject="SPARK Task Failure: File Not Found Scalar_Usage_{file_date}*".format(
        file_date=file_date_format
    ),
    html_content=""" <p><h3>Failure Report: SPARK File Not Found for {file_date}.
    <br /> Action Taken: Re-using yesterday's file.<br />
        <br /> This is an automated email. </h3>
        <br /> From: support@innowatts.com</p>
        """.format(
        file_date=file_date_format
    ),
    # Base
    dag=dag,
)


t_unzip_scalar_usage = UnzipOperator(
    task_id='unzip_scalar_usage',
    path_to_zip_file=local_s3mount + s3_ftp + 'SCALAR_USAGE_{}_NORTHEAST.zip'.format(file_date_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)

t_unzip_yesterday_scalar_usage = UnzipOperator(
    task_id='unzip_yesterday_scalar_usage',
    path_to_zip_file=local_s3mount + s3_ftp + 'SCALAR_USAGE_{}_NORTHEAST.zip'.format(file_yesterday_format),
    path_to_unzip_contents=local_s3mount + s3_ftp,
    dag=dag)


t_move_yesterday_scalar_usage = S3ToS3Operator(
    task_id="move_yesterday_scalar_usage",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_yesterday_scalar_usage),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path=r'{}.csv'.format(raw_source_scalar_usage),
    s3_destination_bucket=s3_ftp_bucket,
    aws_conn_id=aws_conn_id,
    mode="copy",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_move_scalar_usage = S3ToS3Operator(
    task_id="move_scalar_usage",
    pool="blockable",
    queue="default",
    priority_weight=10,
    s3_source_path=r'{}.csv'.format(raw_source_scalar_usage),
    s3_source_bucket=s3_ftp_bucket,
    s3_destination_path='{}SCALAR_USAGE_{}_NORTHEAST.csv'.format(raw_dest_scalar_usage, file_date_format),
    s3_destination_bucket=s3_processing_bucket,
    aws_conn_id=aws_conn_id,
    mode="copy",
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_add_partition_scalar_usage = AwsAthenaOperator(
    query=(
        "ALTER TABLE {}.r_northeast_scalar_usage ADD PARTITION".format(
            rep_code
        )
        + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
        + ' location "s3://{}/{}/"'.format(s3_processing_bucket, raw_dest_scalar_usage)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_scalar_usage",
    trigger_rule='one_success',
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_verify_partition_scalar_usage = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_scalar_usage') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_scalar_usage",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)


#customer_info
start_ops >> find_customer_info_file >> does_customer_info_file_exist >> meter_info_found >> t_unzip_customer_info >> meter_unzip #>> t_move_customer_info >> t_add_partition_customer_info >> t_verify_partition_customer_info >> finish_ops
does_customer_info_file_exist >> meter_info_missing >> customer_info_email_unavailable_file >> t_unzip_yesterdays_customer_info >> t_move_yesterday_customer_info >> meter_unzip
meter_unzip >> t_move_customer_info >> t_add_partition_customer_info >> t_verify_partition_customer_info >> finish_ops


#meter_info
start_ops >> find_meter_info_file >> does_meter_info_file_exist >> meter_attributes_found >> t_unzip_meter_info >> attribute_unzip #>> t_move_meter_info >> t_add_partition_meter_info >> t_verify_partition_meter_info >> finish_ops
does_meter_info_file_exist >> meter_attributes_missing >> meter_info_email_unavailable_file >> t_unzip_yesterday_meter_info >> t_move_yesterday_meter_info >> attribute_unzip
attribute_unzip >> t_move_meter_info >> t_add_partition_meter_info >> t_verify_partition_meter_info >> finish_ops

#scalar
start_ops >> find_scalar_file >> does_scalar_file_exist >> scalar_found >> t_unzip_scalar_usage >> scalar_unzip #>> t_move_scalar_usage
does_scalar_file_exist >> scalar_missing >> scalar_email_unavailable_file >> t_unzip_yesterday_scalar_usage >> t_move_yesterday_scalar_usage >> scalar_unzip
scalar_unzip >> t_move_scalar_usage >> t_add_partition_scalar_usage >> t_verify_partition_scalar_usage >> finish_ops
# t_move_scalar_usage >> t_move_scalar_usage_processed >> t_rename_columns_processed >> t_add_partition_scalar_usage_processed >> t_verify_partition_scalar_usage_processed >> finish_ops
