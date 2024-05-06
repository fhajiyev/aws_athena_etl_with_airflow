import os
import yaml

from airflow import DAG
from datetime import datetime, timedelta
from operators.mysql_s3_operator import MySqlS3Operator
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.constants import RedshiftLoadType
from plugins.redshift_plugin.operators.s3_redshift_operator import S3RedshiftOperator
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from utils.constants import DEFAULT_MYSQL_BATCH_SIZE, REDSHIFT_COPY_OPTIONS
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

from airflow.utils.log.logging_mixin import LoggingMixin

default_args = {
    'owner': 'devop',
    'depends_on_past': False,
    'backfill': True,
    'start_date': datetime(2019, 1, 1),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 1,
    'retries': 5,
}


def create_mysql_s3_redshift_pipeline_dag(
    dag_id,
    default_args,
    pipeline_config,
    schedule_interval,
):
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval or timedelta(minutes=30),
        max_active_runs=1,
        catchup=False,
        default_args=default_args,
    )

    s3_bucket = 'buzzvil-airflow'
    s3_prefix = '/'.join(['sync_mysql_redshift', pipeline_config['pipeline_key']])

    with dag:

        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        retrieve_last_increment_value = RedshiftSchemaOperator(
            table_name=pipeline_config['redshift']['table_name'],
            increment_key=pipeline_config['redshift']['increment_key'],
            retrieve_last_increment_value=pipeline_config['incremental_sync'],
            create_table=True,
            create_table_syntax=pipeline_config['redshift']['create_table_syntax'],

            redshift_conn_id='redshift',
            database='buzzad',

            task_id='retrieve_last_increment_value'
        )

        stage_mysql_data = MySqlS3Operator(
            mysql_conn_id=pipeline_config['mysql']['conn_id'],
            mysql_table_name=pipeline_config['mysql']['table_name'],
            mysql_columns=pipeline_config['mysql']['fields'],
            increment_key_from_xcom=pipeline_config['incremental_sync'],
            increment_key=pipeline_config['mysql']['increment_key'],
            increment_key_type=pipeline_config['mysql']['increment_key_type'],
            xcom_source_task_id='retrieve_last_increment_value',
            s3_conn_id='adfit_s3',
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_prefix_append_dag_run_uuid=True, # Required to prevent dirty writes to s3 intermediate location caused by parallel executions

            batch_size=pipeline_config['mysql'].get('batch_size') or DEFAULT_MYSQL_BATCH_SIZE,
            skip_downstream=True,

            task_id='mysql_s3_migration'
        )

        copy_s3_data = S3RedshiftOperator(
            table_name=pipeline_config['redshift']['table_name'],
            columns=pipeline_config['redshift']['fields'],
            increment_key=pipeline_config['redshift']['increment_key'],
            unique_key_list=pipeline_config['redshift']['unique_key_list'],
            copy_method=RedshiftLoadType(pipeline_config['redshift']['copy_method']),

            redshift_conn_id='redshift',
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,

            copy_option_list=REDSHIFT_COPY_OPTIONS[pipeline_config['redshift'].get('copy_option', 'default_csv')],

            task_id='copy_s3_data'
        )

        generate_uuid >> retrieve_last_increment_value >> stage_mysql_data >> copy_s3_data

    return dag


log = LoggingMixin().log

pipeline_config_files = []

for dir_path, dir_name, file_names in os.walk('pipelines/mysql_redshift'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        dag_id = 'sync_mysql_redshift_{data_key}'.format(data_key=pipeline_config['pipeline_key'])
        if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
        log.info("Processed {0}".format(pipeline_config['pipeline_key']))

        globals()[dag_id] = create_mysql_s3_redshift_pipeline_dag(
            default_args=default_args,
            dag_id=dag_id,

            pipeline_config=pipeline_config,
            schedule_interval=pipeline_config.get('pipeline_dag_configs', {}).get('schedule_interval'),
        )
