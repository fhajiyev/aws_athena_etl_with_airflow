import os
import yaml

from airflow import DAG
from datetime import datetime
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import timedelta
from operators.mysql_s3_operator import MySqlS3Operator
from operators.uuid_generator import UUIDGenerator

from utils.constants import DEFAULT_MYSQL_BATCH_SIZE
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'start_date': datetime(2018, 12, 11),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 1,
    'retries': 5,
}


def create_mysql_s3_load_migration_dag(
    default_args,
    dag_id,
    pipeline_config,
    schedule_interval=None,
):

    dag = DAG(
        dag_id=dag_id,
        max_active_runs=1,
        catchup=True,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    manifest_path = "s3://{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake/manifest"

    with dag:
        execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        next_execution_datehour = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        year = "{{ execution_date.strftime('%Y') }}"
        month = "{{ execution_date.strftime('%m') }}"
        day = "{{ execution_date.strftime('%d') }}"
        hour = "{{ execution_date.strftime('%H') }}"

        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        unload_mysql_data_to_s3 = MySqlS3Operator(
            mysql_conn_id=pipeline_config['mysql']['conn_id'],
            mysql_table_name=pipeline_config['mysql']['table_name'],
            mysql_columns=pipeline_config['mysql']['fields'],
            increment_key=pipeline_config['mysql']['increment_key'],
            increment_key_from=execution_datehour,
            increment_key_to=next_execution_datehour,
            increment_key_type=pipeline_config['mysql']['increment_key_type'],
            s3_conn_id='adfit_s3',
            s3_bucket=pipeline_config['s3']['bucket'],
            s3_prefix=pipeline_config['s3']['prefix'].format(
                year=year,
                month=month,
                day=day,
                hour=hour
            ),
            batch_size=DEFAULT_MYSQL_BATCH_SIZE,
            file_key=pipeline_config['s3']['file_key'],
            data_format=pipeline_config['s3']['data_format'],
            task_id='unload_mysql_data_to_s3',
        )

        reconcile_athena_database = AWSAthenaOperator(
            query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                database=pipeline_config['athena']['database']
            ),
            database=pipeline_config['athena']['database'],
            output_location=manifest_path,
            aws_conn_id='aws_athena',
            task_id='reconcile_athena_database',
        )

        reconcile_athena_table = AWSAthenaOperator(
            query=pipeline_config['athena']['create_table_syntax'].format(
                database=pipeline_config['athena']['database'],
                table=pipeline_config['athena']['table'],
                partition_name=pipeline_config['athena']['partition']['name'],
                location=pipeline_config['athena']['location'],
            ),
            database=pipeline_config['athena']['database'],
            output_location=manifest_path,
            aws_conn_id='aws_athena',
            task_id='reconcile_athena_table',
        )

        partition_athena_table = AWSAthenaOperator(
            query="""
                ALTER TABLE {database}.{table}
                ADD IF NOT EXISTS PARTITION ({partition_name} = '{partition_value}')
                LOCATION '{partition_location}';
            """.format(
                database=pipeline_config['athena']['database'],
                table=pipeline_config['athena']['table'],
                partition_name=pipeline_config['athena']['partition']['name'],
                partition_value=execution_datehour,
                partition_location='/'.join([
                    pipeline_config['athena']['location'],
                    'year={year}'.format(year=year),
                    'month={month}'.format(month=month),
                    'day={day}'.format(day=day),
                    'hour={hour}'.format(hour=hour)
                ]),
            ),
            database=pipeline_config['athena']['database'],
            output_location=manifest_path,
            aws_conn_id='aws_athena',
            task_id='partition_athena_table',
        )

        generate_uuid >> unload_mysql_data_to_s3 >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table

        if pipeline_config.get('delay_seconds'):
            delay_seconds = pipeline_config.get('delay_seconds')
            delay_execution = TimeDeltaSensor(
                delta=timedelta(seconds=delay_seconds),
                timeout=10,
                poke_interval=10,
                retry_delay=timedelta(seconds=delay_seconds),
                task_id='delay_execution'
            )
            delay_execution >> generate_uuid

    return dag


pipeline_config_files = []
extra_params = dict()
schedule_interval = None

for dir_path, dir_name, file_names in os.walk('pipelines/mysql_s3_athena'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'mysql_s3_athena':
            dag_id = 'mysql_s3_athena_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                if 'schedule_interval' in pipeline_config['pipeline_dag_configs'].keys():
                    schedule_interval = pipeline_config['pipeline_dag_configs']['schedule_interval']

            globals()[dag_id] = create_mysql_s3_load_migration_dag(
                default_args=default_args,
                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,

                dag_id=dag_id,
            )
