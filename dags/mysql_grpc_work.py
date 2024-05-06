import datetime
import os

from yaml import load
from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin

from operators.mysql_grpc_operator import MysqlToGrpcOperator
from operators.uuid_generator import UUIDGenerator
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'start_date': datetime.datetime(2019, 7, 1),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 1,
    'backfill': True,
    'retries': 5,
    'concurrency': 1,
}


def create_mysql_grpc_work_dag(
        default_args,
        pipeline_config,
        dag_id,
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
        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        next_execution_datehour = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"

        sql = pipeline_config['mysql']['query'].format(start_from=execution_datehour, end_to=next_execution_datehour)

        grpc_task = MysqlToGrpcOperator(
            sql=sql,
            mysql_conn_id=pipeline_config['mysql']['conn_id'],
            mapper_func=pipeline_config['grpc']['mapper_func'],
            stub_class_func=pipeline_config['grpc']['stub_class_func'],
            call_func=pipeline_config['grpc']['call_func'],
            error_list=pipeline_config['grpc']['error_list'],
            grpc_conn_id=pipeline_config['grpc']['conn_id'],
            task_id='grpc_task',
        )

        generate_uuid >> grpc_task
    return dag


pipeline_config_files = []
extra_params = dict()
schedule_interval = None

for dir_path, dir_name, file_names in os.walk('pipelines/mysql_grpc'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = load(pipeline_config, Loader=Loader)
        if pipeline_config['pipeline_type'] == 'mysql_grpc':
            dag_id = 'mysql_grpc_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                if 'schedule_interval' in pipeline_config['pipeline_dag_configs'].keys():
                    schedule_interval = pipeline_config['pipeline_dag_configs']['schedule_interval']

            globals()[dag_id] = create_mysql_grpc_work_dag(
                default_args=default_args,
                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,

                dag_id=dag_id,
            )
