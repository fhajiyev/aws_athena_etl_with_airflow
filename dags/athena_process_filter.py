import os
import yaml

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from operators.s3_clear_prefix_operator import S3ClearPrefixOperator
from operators.s3_rename_objects_operator import S3RenameObjectsOperator

from operators.uuid_generator import UUIDGenerator
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator

log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'concurrency': 1,
}

manifest_path = "s3://{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake/manifest"


def create_athena_pipeline_dag(
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

        for filter_value in pipeline_config['filter']['values']:
            drop_legacy_temp_table = AWSAthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=pipeline_config['athena']['database'],
                    temp_table='_'.join([pipeline_config['athena']['table'], pipeline_config['filter']['name'],
                                         str(filter_value), 'temp', year, month, day, hour]),
                ),
                database=pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='_'.join(['drop_legacy_temp_table', pipeline_config['filter']['name'], str(filter_value)]),
            )

            drop_legacy_s3_object = S3ClearPrefixOperator(
                bucket=pipeline_config['athena']['output_bucket'],
                prefix=pipeline_config['athena']['output_prefix'].format(
                    filter_name=pipeline_config['filter']['name'],
                    filter_value=filter_value,
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                aws_conn_id='adfit_s3',
                task_id='_'.join(['drop_legacy_s3_object', pipeline_config['filter']['name'], str(filter_value)]),
            )

            proceed_athena_query = AWSAthenaOperator(
                query=pipeline_config['athena']['process_query'].format(
                    database=pipeline_config['athena']['database'],
                    temp_table='_'.join([pipeline_config['athena']['table'], pipeline_config['filter']['name'],
                                         str(filter_value), 'temp', year, month, day, hour]),
                    output_bucket=pipeline_config['athena']['output_bucket'],
                    output_prefix=pipeline_config['athena']['output_prefix'].format(
                        filter_name=pipeline_config['filter']['name'],
                        filter_value=filter_value,
                        year=year,
                        month=month,
                        day=day,
                        hour=hour,
                    ),
                    start_time=execution_datehour,
                    end_time=next_execution_datehour,
                    filter_name=pipeline_config['filter']['name'],
                    filter_value=filter_value,
                ),
                database=pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='_'.join(['proceed_athena_query', pipeline_config['filter']['name'], str(filter_value)]),
            )

            drop_temp_table = AWSAthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=pipeline_config['athena']['database'],
                    temp_table='_'.join([pipeline_config['athena']['table'], pipeline_config['filter']['name'],
                                         str(filter_value), 'temp', year, month, day, hour]),
                ),
                database=pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='_'.join(['drop_temp_table', pipeline_config['filter']['name'], str(filter_value)]),
            )

            rename_s3_objects = S3RenameObjectsOperator(
                bucket=pipeline_config['athena']['output_bucket'],
                prefix=pipeline_config['athena']['output_prefix'].format(
                    filter_name=pipeline_config['filter']['name'],
                    filter_value=filter_value,
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                file_key='{file_key}-{year}-{month}-{day}-{hour}'.format(
                    file_key=pipeline_config['athena']['file_key'].format(
                        filter_name=pipeline_config['filter']['name'],
                        filter_value=filter_value,
                    ),
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                file_extension=pipeline_config['athena']['file_extension'],
                task_id='_'.join(['rename_s3_objects', pipeline_config['filter']['name'], str(filter_value)]),
            )

            generate_uuid >> drop_legacy_temp_table >> proceed_athena_query
            generate_uuid >> drop_legacy_s3_object >> proceed_athena_query
            proceed_athena_query >> drop_temp_table >> rename_s3_objects

    return dag


pipeline_config_files = []
extra_params = dict()
schedule_interval = None

for dir_path, dir_name, file_names in os.walk('pipelines/athena_process_filter'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'athena_process_filter':
            dag_id = '_'.join([pipeline_config['pipeline_type'], pipeline_config['pipeline_key']])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                schedule_interval = pipeline_config.get('pipeline_dag_configs', {}).pop('schedule_interval', None)

                print("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))

            globals()[dag_id] = create_athena_pipeline_dag(
                default_args=default_args,
                dag_id=dag_id,

                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,
            )

            log.info("Processed {0}".format(pipeline_config['pipeline_key']))
