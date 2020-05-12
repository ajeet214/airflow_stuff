"""
# Innowatts Airflow Client Pipeline
### Rep Code: MEGA
### MEGA EXTRACTION
### MEGA - Scalar, Meter_Info
### Expected Arrival (Pull) Time - daily


This DAG retrieves Scalar Usage and Meter Info from an ECI Getdata using


### Author: Ajeet Verma
"""

# External and Base Modules
from datetime import datetime, timedelta
import pandas as pd
import boto3
from io import StringIO, BytesIO

# Airflow Modules
import airflow.macros
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

# Internal Modules
import innowatts_customers.subdags as subdags
from common.utils.slack import slack_on_failure
from common.utils.slack import slack_on_success
from airflow.operators import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.innowattsairflowcore import AwsAthenaOperator
from airflow.operators.innowattsairflowcore import AthenaQuerySensor
from airflow.operators.python_operator import BranchPythonOperator
from innowatts_airflow_core.model.airflow_env_config import Airflow_Env_Config
# from innowatts_customers.sparkenergy.sparkenergy_create_raw_tables import sparkenergy_split_markets
from getdata import getdata

env = Variable.get("env_prefix")
start_date = datetime(2019, 10, 1, 0, 0)
rep_code = '{}_mega'.format(env)
aws_conn_id = 's3-development'
region_name = 'us-east-2'

airflow_env = Airflow_Env_Config(env, ["ajeet.verma@innowatts.com"], True, "0 18   *")

airflow_config = {
    "aws_conn_id": "s3-development",
    "DAG_NAME": "MEGA-eci-extraction2_getdata",
    "default_args": {
        "depends_on_past": False,
        "email": airflow_env.email,
        "email_on_failure": airflow_env.email,
        "email_on_retry": False,
        "owner": "Ajeet",
        "poke_interval": 30,
        "poke_timeout": 1800,
        "retries": 3,
        "retry_delay": timedelta(minutes=30),
        "retry_exponential_backoff": False,
        "start_date": start_date,
    },
    "poke_interval": 30,
    "poke_timeout": 1800,
}


dag = DAG(
    dag_id=airflow_config["DAG_NAME"],
    catchup=airflow_env.catchup,
    concurrency=5,
    default_args=airflow_config["default_args"],
    max_active_runs=1,
    schedule_interval=airflow_env.schedule_interval,
)

dag.doc_md = _doc_

# Task Tuning
default_execution_timeout = timedelta(seconds=45)
default_retry_delay = timedelta(seconds=10)
default_retries = 10
default_retry_exponential_backoff = True
poke_timeout = 1800
poke_interval = 30
unprocessed_s3_staging_full = "s3://internal-innowatts-airflow-staging/Mega/{}/ECI/daily/".format(env)


start_dag = subdags.dummy_operator("MEGA_ECI", airflow_config, dag)

remote_conn = BaseHook.get_connection("mega-eci-db_getdata")
# db = remote_conn.schema
remote = '{}:{}:{}:{}:{}'.format(remote_conn.host, remote_conn.login, remote_conn.password, remote_conn.schema,
                                 remote_conn.port)

s3_conn = BaseHook.get_connection('s3-development')
s3 = '{}:{}'.format(s3_conn.login, s3_conn.password)
s3_processing_bucket = '{}-innowatts-mars-megaenergy-processing'.format(env)

raw_path = 'mega/unprocessed'

meter_info_suffix = 'meter_info_eci'
scalar_usage_suffix = 'scalar_usage_eci'

slack_key = Variable.get('slack_api_token')


process_ts = '{{ (execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S") }}'

partitions_done = DummyOperator(task_id="PARTITIONS_DONE", dag=dag)


def megaenergy_split_markets(**context):
    s3_raw_path = ""
    bytes_to_write = ""

    process_ts = context['templates_dict']['process_ts']
    raw_table_suffix = context['templates_dict']['raw_table_suffix']
    raw_path = context['templates_dict']['raw_path']
    bucket = context['templates_dict']['bucket']
    get_data = getdata.GetData(context['templates_dict']['slack-key'])

    host, user, password, database, port = context['templates_dict']['brands_list'].split(':')
    start_date = process_ts[0:10]
    end_date = (datetime.strptime(process_ts, "%Y-%m-%dT%H:%M:%S") + timedelta(days=1)).strftime("%Y-%m-%d")

    creds = '{}:{}:{}:{}:{}'.format(host, user, password, database, port)

    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key='mega/unprocessed/r_mega_duns_mapping.csv')
        mapping_df = pd.read_csv(BytesIO(obj['Body'].read()))
        mapping_df['duns_number'] = mapping_df['duns_number'].astype(str)
        mapping_df['duns_number'] = mapping_df['duns_number'].apply(lambda x : x.replace("'", ""))
        print(mapping_df, 'cheching mapping df dataframe')
        print('#########', mapping_df.info())
    except Exception as e:
        print('mapping file not found {}'.format(e))
        return False

    df = pd.DataFrame()
    if raw_table_suffix == 'scalar_usage_eci':
        df = get_data.eci.eci_scalar_to_dataframe(start_date, end_date, creds)
        print('@@@@@@@@', df.info())
        
        print('scalar usage eci dataframe : ', df)

        merged_df = df.merge(
                             mapping_df[['duns_number', 'market']],
                             how='inner',
                             left_on='UtilityDunsNumber',
                             right_on='duns_number')

        merged_df.drop('duns_number', axis=1, inplace=True)
       
        print('merged df')
        print(merged_df)
        markets_to_s3(merged_df, bucket, raw_path, raw_table_suffix, process_ts)

    elif raw_table_suffix == 'meter_info_eci':
        df = get_data.eci.eci_meterinfo_to_dataframe(start_date, end_date, creds)
        print('df info', df.info(), df)
        
        df.drop(df[df.UtilityDunsNumber == 'DUNS_NotApplicable'].index, inplace=True)
        
        print('meter info eci dataframe : ', df)

        merged_df = df.merge(
                             mapping_df[['duns_number', 'market']],
                             left_on='UtilityDunsNumber',
                             right_on='duns_number',
                             how='inner')

        merged_df.drop('duns_number', axis=1, inplace=True)
        
        print('merged df')
        print(merged_df)
        markets_to_s3(merged_df, bucket, raw_path, raw_table_suffix, process_ts)


def markets_to_s3(df, bucket, raw_path, suffix, process_ts):
    markets = df['market'].unique().tolist()
    print(markets)

    for m in markets:
        if m == 'nan':
            pass
        else:
            pdate = (datetime.strptime(process_ts, "%Y-%m-%dT%H:%M:%S")).strftime("%Y-%m-%d")
            
            df_store = df.loc[df['market'] == m]
            df_store.drop('market', axis=1, inplace=True)

            
            # upload to S3
            print("Uploading file to s3")
            if suffix == 'scalar_usage_eci':

                s3_raw_path = '{path}/{market}/{suffix}/process_date={pdate}/r_scalar_usage_{fdate}.csv'.format(
                                                                                                                path=raw_path,
                                                                                                                market=m.lower(),
                                                                                                                suffix=suffix,
                                                                                                                pdate=pdate,
                                                                                                                fdate=pdate.replace('-', ''))

                bytes_to_write = df_store.to_csv(index=False).encode()
                print('Saving {} {} raw file to {}'.format(m, suffix, s3_raw_path))

            else:
                # meter info columns in ECI can have text in them that includes commas, using pipe instead

                s3_raw_path = '{path}/{market}/{suffix}/process_date={pdate}/r_meter_info_{fdate}.csv'.format(
                                                                                                                path=raw_path,
                                                                                                                market=m.lower(),
                                                                                                                suffix=suffix,
                                                                                                                pdate=pdate,
                                                                                                                fdate=pdate.replace('-', ''))

                bytes_to_write = df_store.to_csv(index=False, sep='|').encode()
                print('Saving {} {} raw file to {}'.format(m, suffix, s3_raw_path))

            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, s3_raw_path).put(Body=bytes_to_write, ContentType='text/csv')
            print("Uploading done")


## meter info
create_raw_tables_meter = PythonOperator(
    task_id='create_raw_tables_meter',
    priority_weight=10,
    depends_on_past=False,
    queue="whitelist_ip_only",
    pool="default",
    python_callable=megaenergy_split_markets,
    provide_context=True,
    templates_dict={
        'brands_list': remote,
        's3_conn': s3,
        "bucket": s3_processing_bucket,
        "raw_path": raw_path,
        "raw_table_suffix": meter_info_suffix,
        "process_ts": process_ts,
        "slack-key": slack_key},
    dag=dag,
    on_failure_callback=slack_on_failure)

#-----------pjm---------
add_partition_pjm_meter_info_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_pjm_meter_info_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'pjm',
                                                                                                meter_info_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_pjm_meter_info_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_pjm_meter_info_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_pjm_meter_info_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_pjm_meter_info_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

#----------ercot----------
add_partition_ercot_meter_info_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_ercot_meter_info_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'ercot',
                                                                                                meter_info_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_ercot_meter_info_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_ercot_meter_info_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_ercot_meter_info_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_ercot_meter_info_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

#-----------iso-ne---------
add_partition_isone_meter_info_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_isone_meter_info_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'iso-ne',
                                                                                                meter_info_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_isone_meter_info_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_isone_meter_info_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_isone_meter_info_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_isone_meter_info_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)


## scalar usage
create_raw_tables_scalar = PythonOperator(
    task_id='create_raw_tables_scalar',
    priority_weight=10,
    depends_on_past=False,
    queue="whitelist_ip_only",
    pool="default",
    python_callable=megaenergy_split_markets,
    provide_context=True,
    templates_dict={
        'brands_list': remote,
        's3_conn': s3,
        "bucket": s3_processing_bucket,
        "raw_path": raw_path,
        "raw_table_suffix": scalar_usage_suffix,
        "process_ts": process_ts,
        "slack-key": slack_key},
    dag=dag,
    on_failure_callback=slack_on_failure)

#-----------pjm---------
add_partition_pjm_scalar_usage_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_pjm_scalar_usage_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'pjm',
                                                                                                scalar_usage_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_pjm_scalar_usage_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_pjm_scalar_usage_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_pjm_scalar_usage_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_pjm_scalar_usage_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

#----------ercot----------
add_partition_ercot_scalar_usage_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_ercot_scalar_usage_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'ercot',
                                                                                                scalar_usage_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_ercot_scalar_usage_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_ercot_scalar_usage_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_ercot_scalar_usage_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_ercot_scalar_usage_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

#-----------iso-ne---------
add_partition_isone_scalar_usage_eci = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_isone_scalar_usage_eci ADD IF NOT EXISTS PARTITION".format(
                rep_code
            )
            + ' (process_date="{{ macros.ds_add( ds, 1 ) }}")'
            + ' location "s3://{}/{}/{}/{}/process_date={{{{ macros.ds_add( ds, 1 ) }}}}/"'.format(s3_processing_bucket,
                                                                                                raw_path,
                                                                                                'iso-ne',
                                                                                                scalar_usage_suffix)
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_isone_scalar_usage_eci",
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

verify_partition_isone_scalar_usage_eci = AthenaQuerySensor(
    query_execution_id="{{ task_instance.xcom_pull(task_ids='add_partition_isone_scalar_usage_eci') }}.csv",
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_isone_scalar_usage_eci",
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)


start_dag >> create_raw_tables_meter
create_raw_tables_meter >> add_partition_pjm_meter_info_eci >> verify_partition_pjm_meter_info_eci >> partitions_done
create_raw_tables_meter >> add_partition_ercot_meter_info_eci >> verify_partition_ercot_meter_info_eci >> partitions_done
create_raw_tables_meter >> add_partition_isone_meter_info_eci >> verify_partition_isone_meter_info_eci >> partitions_done

start_dag >> create_raw_tables_scalar
create_raw_tables_scalar >> add_partition_pjm_scalar_usage_eci >> verify_partition_pjm_scalar_usage_eci >> partitions_done
create_raw_tables_scalar >> add_partition_ercot_scalar_usage_eci >> verify_partition_ercot_scalar_usage_eci >> partitions_done
create_raw_tables_scalar >> add_partition_isone_scalar_usage_eci >> verify_partition_isone_scalar_usage_eci >> partitions_done
