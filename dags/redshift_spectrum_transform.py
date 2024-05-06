import os
import yaml

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from plugins.redshift_plugin.operators.redshift_spectrum_operator import RedshiftSpectrumOperator
from utils.constants import XcomParam
from utils.slack import task_fail_slack_alert, task_retry_slack_alert


log = LoggingMixin().log

default_args = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'start_date': datetime(2019, 2, 1, 0),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'concurrency': 1,
}


def create_redshift_spectrum_pipeline_dag(
    default_args,
    dag_id,
    pipeline_config,
    schedule_interval=None
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

        if schedule_interval == '@daily':
            slashed_execution_datehour = "{{ execution_date.strftime('%Y/%m/%d') }}"
        elif schedule_interval == '@hourly':
            slashed_execution_datehour = "{{ execution_date.strftime('%Y/%m/%d/%H') }}"
        else:
            slashed_execution_datehour = "{{ execution_date.strftime('%Y/%m/%d/%H') }}"

        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        reconcile_spectrum_table = RedshiftSpectrumOperator(
            table_name=pipeline_config['athena']['table_name'],

            create_table=True,
            create_table_syntax=pipeline_config['athena']['create_table_syntax'],

            add_partition=True,
            partition_name=pipeline_config['athena']['partition']['name'],
            partition_value=execution_datehour,
            partition_location='/'.join([
                pipeline_config['athena']['partition']['location'],
                slashed_execution_datehour
            ]),

            task_id='reconcile_spectrum_table',
        )

        reconcile_redshift_table = RedshiftSchemaOperator(
            table_name=pipeline_config['redshift']['table_name'],
            create_table=True,
            create_table_syntax=pipeline_config['redshift']['create_table_syntax'],
            retrieve_last_increment_value=False,
            redshift_conn_id='redshift',
            database='buzzad',

            task_id='reconcile_redshift_table',
        )

        transform_reload_data = RedshiftOperator(
            sql="""
                {% raw %}
                BEGIN;
                CREATE TABLE IF NOT EXISTS {param[table_name]}_staging_{{param[dag_run_uuid]}} (LIKE {param[table_name]});

                INSERT INTO {param[table_name]}_staging_{{param[dag_run_uuid]}}
                (
                    {param[transform_subquery]}
                );

                DELETE FROM {param[table_name]}
                USING {param[table_name]}_staging_{{param[dag_run_uuid]}}
                WHERE {param[unique_condition]};

                INSERT INTO {param[table_name]}
                (
                    SELECT
                        {param[columns]}
                    FROM
                        (
                        SELECT
                            *, ROW_NUMBER() OVER (PARTITION BY {param[deduplicate_by]} ORDER BY {param[increment_key]} DESC) AS rownum
                        FROM
                            {param[table_name]}_staging_{{param[dag_run_uuid]}}
                        )
                    WHERE rownum = 1
                );
                DROP TABLE {param[table_name]}_staging_{{param[dag_run_uuid]}};
                END;
                {% endraw %}
                """,
            redshift_conn_id='redshift',
            xcom_params=[
                XcomParam(xcom_source_task_id='generate_uuid', xcom_param_key='dag_run_uuid'),
            ],
            param_dict=dict({
                'table_name': pipeline_config['redshift']['table_name'],
                'columns': ', '.join(pipeline_config['redshift']['fields']),
                'deduplicate_by': ', '.join(pipeline_config['redshift']['deduplicate_key_list']),
                'increment_key': pipeline_config['redshift']['increment_key'],
                'transform_subquery': pipeline_config['transform']['select_query'].format(
                    start_time=execution_datehour,
                    end_time=next_execution_datehour,
                ),
                'unique_condition': ' AND '.join(
                    '{table_name}.{key} = {table_name}_staging_{{param[dag_run_uuid]}}.{key}'.format(
                        table_name=pipeline_config['redshift']['table_name'],
                        key=key,
                    ) for key in pipeline_config['redshift']['unique_key_list']
                )
            }),

            task_id='transform_reload_data',
        )
        generate_uuid >> reconcile_spectrum_table >> reconcile_redshift_table >> transform_reload_data

    return dag


pipeline_config_files = []
extra_params = dict()
schedule_interval = None

for dir_path, dir_name, file_names in os.walk('pipelines/spectrum_process'):
    pipeline_config_files.extend('/'.join([dir_path, f]) for f in file_names)

for config_file in pipeline_config_files:
    with open(config_file, 'r') as pipeline_config:
        pipeline_config = yaml.load(pipeline_config)
        if pipeline_config['pipeline_type'] == 'redshift_spectrum_transform':
            dag_id = 'redshift_spectrum_transform_hourly_{data_key}'.format(data_key=pipeline_config['pipeline_key'])

            if 'pipeline_dag_configs' in pipeline_config.keys():
                log.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))
                schedule_interval = pipeline_config.get('pipeline_dag_configs', {}).get('schedule_interval')

            if 'deduplicate_key_list' not in pipeline_config['redshift'].keys():
                pipeline_config['redshift']['deduplicate_key_list'] = pipeline_config['redshift']['unique_key_list']

            globals()[dag_id] = create_redshift_spectrum_pipeline_dag(
                default_args=default_args,
                dag_id=dag_id,

                pipeline_config=pipeline_config,
                schedule_interval=schedule_interval,
            )

            log.info("Processed {0}".format(pipeline_config['pipeline_key']))
