import os
import yaml

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from operators.uuid_generator import UUIDGenerator

from datetime import datetime
from typing import List
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'start_date': datetime(2018, 12, 11),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 1,
}


def step_serializer(step_name: str, script_location: str, execution_timestamp: str, next_execution_timestamp: str, args: List[str]):
    step = {
        'Name': step_name,
        'ActionOnFailure': 'TERMINATE_JOB_FLOW',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                script_location,
                '--execution_timestamp', execution_timestamp,
                '--next_execution_timestamp', next_execution_timestamp,
            ]
        }
    }
    if isinstance(args, list):
        step['HadoopJarStep']['Args'].extend(args)
    return step


def create_emr_spark_transform_migration_dag(
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
        execution_timestamp = "{{ execution_date.strftime('%Y-%m-%d %H:%M:%S') }}"
        next_execution_timestamp = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"

        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        create_job_flow = EmrCreateJobFlowOperator(
            task_id='create_job_flow',

            aws_conn_id='s3_default',
            emr_conn_id='adfit_emr',
            job_flow_overrides=pipeline_config['job_flow']['overrides'],
            region_name='us-west-2',
        )

        add_steps = EmrAddStepsOperator(
            task_id='add_steps',

            aws_conn_id='adfit_emr_steps',
            region_name='us-west-2',
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            steps=[
                step_serializer(
                    step_name=step['name'],
                    script_location=step['script_location'],
                    execution_timestamp=execution_timestamp,
                    next_execution_timestamp=next_execution_timestamp,
                    args=step.get('args')
                ) for step in pipeline_config['steps']
            ]
        )

        sense_steps = EmrStepSensor(
            task_id='sense_steps',

            aws_conn_id='adfit_emr_steps',
            region_name='us-west-2',
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0]}}",
        )

        terminate_job_flow = EmrTerminateJobFlowOperator(
            task_id='terminate_job_flow',

            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            aws_conn_id='adfit_emr_steps',
            region_name='us-west-2',
            trigger_rule='all_done',
        )

        generate_uuid >> create_job_flow >> add_steps >> sense_steps >> terminate_job_flow

    return dag


# TODO : Refactor using DAG Factory
pipeline_config_files = []
extra_params = dict()
schedule_interval = None


for dir_path, dir_name, file_names in os.walk('pipelines/emr_spark_transform'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'emr_spark_transform':
            dag_id = 'emr_spark_transform_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                if 'schedule_interval' in pipeline_config['pipeline_dag_configs'].keys():
                    schedule_interval = pipeline_config['pipeline_dag_configs']['schedule_interval']

            globals()[dag_id] = create_emr_spark_transform_migration_dag(
                default_args=default_args,
                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,

                dag_id=dag_id,
            )
