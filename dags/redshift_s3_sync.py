import os
import yaml

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.operators.redshift_unload_operator import RedshiftUnloadOperator
from utils.constants import REDSHIFT_UNLOAD_OPTIONS
from utils.slack import task_fail_slack_alert, task_retry_slack_alert
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'concurrency': 1,
}


def create_redshift_s3_unload_dag(
    default_args,
    dag_id,
    pipeline_config,
    schedule_interval=None,
):

    dag = DAG(dag_id, catchup=True, default_args=default_args, schedule_interval=schedule_interval)

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

        unload_redshift_data_to_s3 = RedshiftUnloadOperator(
            s3_bucket=pipeline_config['s3']['bucket'],
            s3_prefix='/'.join([
                pipeline_config['s3']['prefix'],
                'year={year}'.format(year=year),
                'month={month}'.format(month=month),
                'day={day}'.format(day=day),
                'hour={hour}'.format(hour=hour)
            ]),
            query=pipeline_config['redshift']['query_syntax'].format(start_time=execution_datehour, end_time=next_execution_datehour),
            unload_option_list=REDSHIFT_UNLOAD_OPTIONS['default_parquet'],
            task_id='unload_redshift_data_to_s3',
        )

        reconcile_athena_database = AWSAthenaOperator(
            query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                database=pipeline_config['athena']['database']
            ),
            database=pipeline_config['athena']['database'],
            output_location=manifest_path,
            sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
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
            sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
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
                partition_value=pipeline_config['athena']['partition']['value'],
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
            sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
            aws_conn_id='aws_athena',
            task_id='partition_athena_table',
        )

        generate_uuid >> unload_redshift_data_to_s3 >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table

    return dag


pipeline_config_files = []
extra_params = dict()

for dir_path, dir_name, file_names in os.walk('pipelines/redshift_s3'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'redshift_s3_sync':
            dag_id = 'redshift_s3_sync_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                schedule_interval = pipeline_config.get('pipeline_dag_configs', {}).get('schedule_interval')

            globals()[dag_id] = create_redshift_s3_unload_dag(
                default_args=default_args,
                dag_id=dag_id,

                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,
            )

            log.info("Processed {0}".format(pipeline_config['pipeline_key']))