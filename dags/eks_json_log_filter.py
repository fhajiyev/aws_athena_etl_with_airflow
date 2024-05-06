import os
import yaml
import urllib

from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.aws_sqs_sensor import SQSSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from ast import literal_eval
from operators.uuid_generator import UUIDGenerator

from datetime import datetime
from plugins.emr_plugin.operators.emr_add_steps_callable_operator import EmrAddStepsCallableOperator
from utils.slack import task_fail_slack_alert, task_retry_slack_alert

log = LoggingMixin().log

default_args = {
    'owner': 'data_ops',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 11),
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 3,
}


def json_filter_steps_serializer(**kwargs):
    steps = []
    for message in process_xcom_messages(**kwargs):
        steps.append({
            'Name': 'Filter JSON Logs',
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--master', 'yarn',
                    kwargs['script_location'],
                    '--input_file_location', message,
                ]
            }
        })
    return steps


def process_xcom_messages(**kwargs) -> str:
    # Upstream_task_ids is of type Set()
    messages = kwargs['context']['ti'].xcom_pull(kwargs['context']['ti'].task.upstream_task_ids.pop(), key='messages')
    log.info(messages)
    if messages:
        yield from [process_sqs_record(sqs_record=message) for message in messages['Messages']]
    else:
        log.info("No message to process")


def process_sqs_record(sqs_record, ) -> str:
    # Note that there are only single sns record for each SQS message
    record = literal_eval(literal_eval(sqs_record['Body'])['Message'])['Records'][0]

    src_s3_bucket = record['s3']['bucket']['name']
    src_s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

    return f"s3://{src_s3_bucket}/{src_s3_key}"


def create_eks_json_filter_dag(
    default_args,
    dag_id,
    pipeline_config,
    schedule_interval=None,
):
    dag = DAG(
        dag_id=dag_id,
        max_active_runs=3,
        catchup=True,
        default_args=default_args,
        schedule_interval=schedule_interval,
    )

    with dag:

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

        terminate_job_flow = EmrTerminateJobFlowOperator(
            task_id='terminate_job_flow',

            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            aws_conn_id='adfit_emr_steps',
            region_name='us-west-2',
            trigger_rule='all_done',
        )

        create_job_flow >> generate_uuid

        for i in range(0, pipeline_config.get('add_step_concurrency', 1)):
            sense_new_events = SQSSensor(
                task_id=f'sense_new_events_{i}',

                sqs_queue=pipeline_config['sqs']['queue_url'],
                aws_conn_id='aws_adfit',
                max_messages=pipeline_config.get('max_messages', 10),
                wait_time_seconds=1,
            )

            add_steps = EmrAddStepsCallableOperator(
                task_id=f'add_steps_{i}',

                aws_conn_id='adfit_emr_steps',
                region_name='us-west-2',
                job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
                steps_callable=json_filter_steps_serializer,
                callable_kwargs={
                    'script_location': pipeline_config['script_location'],
                },
            )

            sense_steps = EmrStepSensor(
                task_id=f'sense_steps_{i}',

                aws_conn_id='adfit_emr_steps',
                region_name='us-west-2',
                job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
                step_id=f"{{{{ task_instance.xcom_pull('add_steps_{i}', key='return_value')[-1]}}}}",
            )
            generate_uuid >> sense_new_events >> add_steps >> sense_steps >> terminate_job_flow

    return dag


# TODO : Refactor using DAG Factory
pipeline_config_files = []
extra_params = dict()
schedule_interval = None


for dir_path, dir_name, file_names in os.walk('pipelines/eks_json_log_filter'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'eks_json_log_filter':
            dag_id = 'eks_json_log_filter_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                if 'schedule_interval' in pipeline_config['pipeline_dag_configs'].keys():
                    schedule_interval = pipeline_config['pipeline_dag_configs']['schedule_interval']

            globals()[dag_id] = create_eks_json_filter_dag(
                default_args=default_args,
                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,

                dag_id=dag_id,
            )
