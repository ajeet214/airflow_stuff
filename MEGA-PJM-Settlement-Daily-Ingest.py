"""
# Innowatts Airflow Client Pipeline
### Rep Code: Mega
### Mega PJM - Settlement
### Expected Arrival (Pull) Time - daily 

This DAG retrieves settlement usage data from PJM Settlement API using the getdata package.

### Author: Ajeet Verma
"""

# External and Base Modules
import json
from datetime import datetime, timedelta
from getdata import getdata

# Airflow Modules
import airflow.macros
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from common.operators.emr_add_steps_operator import EmrAddStepsOperatorMod
from common.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperatorMod
from common.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperatorMod
from common.sensors.emr_step_sensor import EmrStepSensorMod

from airflow.operators.innowattsairflowcore import S3ToS3Operator
from airflow.operators.innowattsairflowcore import AwsAthenaOperator
from airflow.operators.innowattsairflowcore import AthenaQuerySensor

import innowatts_customers.subdags as subdags

# Internal Modules
from common.utils.slack import slack_on_failure
from common.utils.slack import slack_on_success
from innowatts_airflow_core.model.airflow_env_config import Airflow_Env_Config
from airflow.macros.innowattsairflowcore import find_file_bash_command


env = Variable.get('env_prefix')
airflow_env = Airflow_Env_Config(env, ["ajeet.verma@innowatts.com"], True, '0 22   *')  # at 23:00 UTC
code_prefix = Variable.get('code_prefix')

start_date = datetime(2019, 10, 24, 0, 0)

default_args = {
    'owner': 'ajeet',
    'depends_on_past': False,
    'start_date': start_date,
    'email': airflow_env.email,
    'email_on_failure': airflow_env.email_on_failure,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}

dag = DAG('MEGA-PJM-Settlement-Daily-Ingest',
          catchup=airflow_env.catchup,
          schedule_interval=airflow_env.schedule_interval,
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
rep_code = '{}_mega'.format(env)
staging_config = Variable.get("staging_config")

s3_conn = BaseHook.get_connection('s3-development')
s3 = '{}:{}'.format(s3_conn.login, s3_conn.password)
#s3_ftp_bucket = '{}-innowatts-mars-megaenergy-ftp'.format(env)
s3_processing_bucket = '{}-innowatts-mars-megaenergy-processing'.format(env)
mount_path = 'mega-processing'
s3_buckets = '/s3buckets/'
raw_path = '/unprocessed/'

pjm_customer_id_cred_list = Variable.get('mega_pjm_settlement_api', deserialize_json=True)

settlement_table_prefix = 'r_pjm_settlements_usage_'
hive_outgoing_settlement_table_name = 'p_pjm_settlements_usage_api'

unprocessed_s3_staging_full = "s3://internal-innowatts-airflow-staging/Mega/{}/PJM/daily/".format(env)
slack_key = Variable.get('slack_api_token')

# process_ts = '{{ (execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")  | localtz("US/Central", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:00:00") }}'
process_ts = '{{ (execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S") }}'


# def file_exists(missing_branch, found_branch, **context):
#     task_string = context["templates_dict"]["task_string"]
#     # logging.info("This is the return value from the find_rt_file task: {}:".format(task_string))
#     if task_string[:9] == "NOT FOUND" or task_string == 'None':
#         branch = missing_branch
#     else:
#         branch = found_branch
#     return branch


def mega_upload_pjm_settlement_api_files(**context):
    start_date = context['templates_dict']['run_date']
    pjm_customer_id_cred_list = context['templates_dict']['pjm_customer_id_cred_list']
    s3_path = context['templates_dict']['s3_path']
    s3_bucket = context['templates_dict']['s3_bucket']
    s3_conn = context['templates_dict']['s3_conn']
    region = context['templates_dict']['region']
    slack_key = context['templates_dict']['slack_key']


    get_data = getdata.GetData(slack_key)
    m,d,y = start_date.split('/')
    full_path = '{}/process_date={}/settlements_usage_api_{}_pjm.csv'.format(s3_path, '{}-{}-{}'.format(y,m,d), '{}{}{}'.format(y,m,d))
    dates_list = []
    dates_list = get_data.pjm.pjm_extract_settlement(start_date, full_path, s3_bucket, s3_conn, region, json.dumps(pjm_customer_id_cred_list))
    if len(dates_list):
        datetime_list = ['{}T00:00:00'.format(d) for d in dates_list]
        datetime_list.sort(reverse=True) # cannot return here, returns None
        print('returning', datetime_list)
        return datetime_list

    return dates_list


# Create Raw File from PJM Settlement API
t_create_pjm_settlement_api_raw_files = PythonOperator(
    task_id='create_pjm_settlement_api_raw_files',
    xcom_push=True,
    priority_weight=10,
    depends_on_past=False,
    queue="whitelist_ip_only",
    pool="default",
    python_callable=mega_upload_pjm_settlement_api_files,
    provide_context=True,
    templates_dict={
        "run_date": "{{ macros.ds_format( macros.ds_add( ds, 1), '%Y-%m-%d', '%m/%d/%Y') }}",
        "pjm_customer_id_cred_list": pjm_customer_id_cred_list,
        "s3_path": 'pjm/unprocessed/r_pjm_settlements_usage_api',
        "s3_bucket": s3_processing_bucket,
        "s3_conn": s3,
        "region": region_name,
        "slack_key": slack_key
    },
    dag=dag,
    on_failure_callback=slack_on_failure)

market = 'pjm'
email_group = ["support@innowatts.com", "monna@innowatts.com"]

partitions_done = DummyOperator(task_id="PARTITIONS_DONE", trigger_rule="all_done", dag=dag)
raw_files_done = DummyOperator(task_id="RAW_FILES_DONE", dag=dag)

t_create_pjm_settlement_api_raw_files >> raw_files_done


# file_missing = DummyOperator(task_id="MISSING_{}_{}".format(market.upper(), 'settlements_usage_api'.upper()), dag=dag,
#                                  on_success_callback=slack_on_success)
# file_found = DummyOperator(task_id="FOUND_{}_{}".format(market.upper(), 'settlements_usage_api'.upper()), dag=dag)


# t_find_file = BashOperator(
#         bash_command=(find_file_bash_command('{mount}{unp}*'.format(mount=s3_buckets,
#                                                                     unp="{}/{}{}r_{}_{}/".format(mount_path, market,
#                                                                                                  raw_path, market,
#                                                                                                  'settlements_usage_api')),
#                                              '{prefix}_{filedate}*'.format(prefix='settlements_usage_api',
#                                             filedate="{{ macros.ds_format( macros.ds_add( ds, 1), '%Y-%m-%d', '%Y%m%d') }}"))),
#         xcom_push=True,
#         # Base Params
#         task_id="find_{}_{}_file".format(market, 'settlements_usage_api'),
#         priority_weight=10,
#         depends_on_past=False,
#         pool="default",
#         queue="default",
#         retries=0,
#         dag=dag,
#         on_failure_callback=slack_on_failure,
#     )
#
# t_does_file_exist = BranchPythonOperator(
#     provide_context=True,
#     python_callable=file_exists,
#     templates_dict={
#         "task_string": "{{{{ task_instance.xcom_pull(task_ids='find_{}_{}_file') }}}}".format(market,
#                                                                                               'settlements_usage_api')
#     },
#     # Base Operators
#     task_id="does_{}_{}_file_exist".format(market, 'settlements_usage_api'),
#     op_kwargs={'missing_branch': "MISSING_{}_{}".format(market.upper(), 'settlements_usage_api'.upper()),
#                'found_branch': "FOUND_{}_{}".format(market.upper(), 'settlements_usage_api'.upper())},
#     trigger_rule="all_done",
#     dag=dag,
# )

t_add_partition = AwsAthenaOperator(
    query=(
            "ALTER TABLE {}.r_{}_{} ADD IF NOT EXISTS PARTITION".format(
                rep_code, market, 'settlements_usage_api'.lower()
            )
            + ' (process_date="{{ macros.ds_add( ds, 1) }}")'
            + ' location "s3://{}/{}{}r_{}_{}/process_date={}/"'.format(s3_processing_bucket, market, raw_path,
                                                                        market, 'settlements_usage_api',
                                                                        "{{  macros.ds_add( ds, 1) }}")
    ),
    db=rep_code,
    s3_output=unprocessed_s3_staging_full,
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="add_partition_{}_{}".format(market, 'settlements_usage_api'),
    pool="athena",
    queue="default",
    priority=12,
    dag=dag,
    on_failure_callback=slack_on_failure,
)

t_verify_partition = AthenaQuerySensor(
    query_execution_id="{{{{ task_instance.xcom_pull(task_ids='add_partition_{}_{}') }}}}.csv".format(market,
                                                                                                      'settlements_usage_api'),
    region_name=region_name,
    aws_conn_id=aws_conn_id,
    # Base Operator Params
    task_id="verify_partition_{}_{}".format(market, 'settlements_usage_api'),
    pool="blocking",
    queue="default",
    priority=10,
    poke_interval=poke_interval,
    timeout=poke_timeout,
    soft_fail=False,
    dag=dag,
)

raw_files_done >> t_add_partition >> t_verify_partition >> partitions_done
# raw_files_done >>  t_find_file >> t_does_file_exist >> file_found >> t_add_partition >> t_verify_partition >> partitions_done
# t_does_file_exist >> file_missing


# create settlement processed table in athena
SPARK_INGEST_STEPS = [
    {
    'Name': 'MEGA_PJM_Daily_Ingest_Settlement_API',
    'ActionOnFailure': 'TERMINATE_JOB_FLOW',
    'HadoopJarStep': {
      'Jar': 'command-runner.jar',
      'Args': [
        'spark-submit',
        '--deploy-mode',
        'cluster',
        '{}spark/common/ingest/getdata/pjm_settlement_ingest.py'.format(Variable.get('code_prefix')),
        '-t',
        '{}'.format(process_ts),
        '-R',
        '{}'.format(rep_code),
        '-s',
        'n',	
        '-B',
        '{}'.format(s3_processing_bucket),
        '-M',
        '{}'.format('pjm'),
        '-I',
        '{}'.format('r_pjm_settlements_usage_api'),
        '-O',
        '{}'.format(hive_outgoing_settlement_table_name),
        '-slack-key',
        '{}'.format(slack_key),
      ]
    }
  }
]


JOB_FLOW_OVERRIDES = {
  "Name": "MEGA PJM Settlement Daily Ingest Job {}".format(airflow_env.emr_env),
  "LogUri": "s3://aws-logs-518589827086-us-east-2/elasticmapreduce/",
  "ReleaseLabel": "emr-5.20.0",
  "Instances": {
    "InstanceGroups": [{
      "Name": "EMR Master nodes",
      "Market": "ON_DEMAND",
      "InstanceRole": "MASTER",
      "InstanceType": "m4.2xlarge",
      "InstanceCount": 1,
      "Configurations": [
        {
          "Classification": "spark",
          "Properties": {
            "maximizeResourceAllocation": "true"
          },
          "Configurations": []
        }, {
          "Classification": "yarn-site",
          "Properties": {
            "yarn.nodemanager.vmem-check-enabled": "false"
          }
        }, {
          "Classification": "spark-hive-site",
          "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
          },
          "Configurations": []
        }, {
          "Classification": "spark-env",
          "Properties": {},
          "Configurations": [
            {
              "Classification": "export",
              "Properties": {
                "PYSPARK_PYTHON": "/usr/bin/python3",
                "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
              },
              "Configurations": []
            }
          ]
        }
      ]
      }, {
        "Name": "EMR Slave nodes",
        "Market": "ON_DEMAND",
        "InstanceRole": "CORE",
        "InstanceType": "m4.2xlarge",
        "InstanceCount": 2,
        "Configurations": [
          {
            "Classification": "spark",
            "Properties": {
              "maximizeResourceAllocation": "true"
            },
            "Configurations": []
          }, {
            "Classification": "yarn-site",
            "Properties": {
              "yarn.nodemanager.vmem-check-enabled": "false"
            }
          }, {
            "Classification": "spark-hive-site",
            "Properties": {
              "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
            "Configurations": []
          }, {
            "Classification": "spark-env",
            "Properties": {},
            "Configurations": [
              {
                "Classification": "export",
                "Properties": {
                  "PYSPARK_PYTHON": "/usr/bin/python3",
                  "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                },
                "Configurations": []
              }
            ]
          }
        ]
      }
    ],
    "Ec2KeyName": "AWS-DevOps-Key",
    "KeepJobFlowAliveWhenNoSteps": True,
    "TerminationProtected": False,
    "Ec2SubnetId": "subnet-6d6bf104"
  },
  "BootstrapActions": [
      {
         "Name": "pyspark_bootstrap",
         "ScriptBootstrapAction": {
                "Path": "{}spark/remote_config/pyspark_bootstrap.sh".format(code_prefix)
        }
      }
  ],
  "Applications": [{
      "Name": "Spark"
    }, {
      "Name": "Ganglia"
    }, {
      "Name": "Hadoop"
    }
  ],
  "JobFlowRole": "EMR_EC2_DefaultRole",
  "ServiceRole": "EMR_DefaultRole",
  "Tags": [
        {
            'Key': 'ENV',
            'Value': 'Prod'
        },
        {
            'Key': 'FACING',
            'Value': 'Internal'
        },
        {
            'Key': 'TECHSTACK',
            'Value': 'Pyspark'
        },
        {
            'Key': 'OWNER',
            'Value': 'DataEngineering'
        },
        {
            'Key': 'PROJECT',
            'Value': 'MEGA-PJM-Settlement-Daily-Ingest'
        },
  ],
  "VisibleToAllUsers": True,
}




t_ingest_cluster_creator = EmrCreateJobFlowOperatorMod(
    task_id='create_ingest_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    region_name='us-east-2',
    aws_conn_id=aws_conn_id,
    emr_conn_id=emr_conn_id,
    pool='default',
    queue='default',
    priority_weight=10,
    retry_delay=30,
    dag=dag,
    on_failure_callback=slack_on_failure)

t_ingest_step_adder = EmrAddStepsOperatorMod(
    task_id='add_ingest_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_ingest_job_flow', key='return_value') }}",
    region_name='us-east-2',
    aws_conn_id=aws_conn_id,
    pool='default',
    queue='default',
    priority_weight=15,
    retry_delay=30,
    steps=SPARK_INGEST_STEPS,
    dag=dag,
    on_failure_callback=slack_on_failure)

t_ingest_step_checker = EmrStepSensorMod(
    task_id='ingest_watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_ingest_job_flow', key='return_value') }}",
    region_name='us-east-2',
    pool='default',
    queue='default',
    priority_weight=12,
    sla=timedelta(hours=1),
    retry_delay=30,
    step_id="{{ task_instance.xcom_pull('add_ingest_steps', key='return_value')[-1] }}",
    aws_conn_id=aws_conn_id,
    dag=dag,
    on_failure_callback=slack_on_failure)

t_ingest_cluster_terminator = EmrTerminateJobFlowOperatorMod(
    task_id='terminate_ingest_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_ingest_job_flow', key='return_value') }}",
    region_name='us-east-2',
    pool='default',
    queue='default',
    priority_weight=20,
    retry_delay=30,
    aws_conn_id=aws_conn_id,
    dag=dag,
    on_failure_callback=slack_on_failure)


partitions_done >> t_ingest_cluster_creator >> t_ingest_step_adder >> t_ingest_step_checker >> t_ingest_cluster_terminator
