import glob

from airflow import DAG
from operators.athena_operator import AthenaOperator
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from operators.s3_rename_objects_operator import S3RenameObjectsOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


ATHENA_PROCESS_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class AthenaProcessDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 3),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 3),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        manifest_path = "s3://{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake/manifest"

        with dag:
            execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            next_execution_datehour = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            year = self.pipeline_config['athena'].get(
                "year", "{{ execution_date.strftime('%Y') }}")
            month = self.pipeline_config['athena'].get(
                "month", "{{ execution_date.strftime('%m') }}")
            day = self.pipeline_config['athena'].get(
                "day", "{{ execution_date.strftime('%d') }}")
            hour = self.pipeline_config['athena'].get(
                "hour", "{{ execution_date.strftime('%H') }}")


            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            drop_legacy_temp_table = AthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=self.pipeline_config['athena']['database'],
                    temp_table='_'.join(
                        [self.pipeline_config['athena']['table'], 'temp', year, month, day, hour]),
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='drop_legacy_temp_table',
            )

            proceed_athena_query = AthenaOperator(
                clean_path=True,
                bucket=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'].format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                query=self.pipeline_config['athena']['process_query'].format(
                    database=self.pipeline_config['athena']['database'],
                    temp_table='_'.join(
                        [self.pipeline_config['athena']['table'], 'temp', year, month, day, hour]),
                    output_bucket=self.pipeline_config['athena']['output_bucket'],
                    output_prefix=self.pipeline_config['athena']['output_prefix'].format(
                        year=year,
                        month=month,
                        day=day,
                        hour=hour,
                    ),
                    start_time=execution_datehour,
                    end_time=next_execution_datehour,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='proceed_athena_query',
            )

            drop_temp_table = AthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=self.pipeline_config['athena']['database'],
                    temp_table='_'.join(
                        [self.pipeline_config['athena']['table'], 'temp', year, month, day, hour]),
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='drop_temp_table',
            )

            rename_s3_objects = S3RenameObjectsOperator(
                bucket=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'].format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                file_key='{file_key}-{year}-{month}-{day}-{hour}'.format(
                    file_key=self.pipeline_config['athena']['file_key'],
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                file_extension=self.pipeline_config['athena']['file_extension'],
                task_id='rename_s3_objects',
            )

            check_s3_object_presence = S3PrefixSensor(
                bucket_name=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'].format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour,
                ),
                poke_interval=1,
                timeout=10,
                soft_fail=self.pipeline_config.get('sensor_soft_fail', True),
                aws_conn_id='adfit_s3',
                task_id='check_s3_object_presence',
            )

            reconcile_athena_database = AthenaOperator(
                query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                    database=self.pipeline_config['athena']['database']
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='reconcile_athena_database',
            )

            reconcile_athena_table = AthenaOperator(
                query=self.pipeline_config['athena']['create_table_syntax'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    partition_name=self.pipeline_config['athena']['partition']['name'],
                    location=self.pipeline_config['athena']['location'],
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='reconcile_athena_table',
            )

            partition_athena_table = AthenaOperator(
                query="""
                    ALTER TABLE {database}.{table}
                    ADD IF NOT EXISTS PARTITION ({partition_name} = '{partition_value}')
                    LOCATION '{partition_location}';
                """.format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    partition_name=self.pipeline_config['athena']['partition']['name'],
                    partition_value=self.pipeline_config['athena']['partition']['value'],
                    partition_location=self.pipeline_config['athena']['partition']['location'].format(
                        year=year,
                        month=month,
                        day=day,
                        hour=hour,
                    ),
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='partition_athena_table',
            )

            generate_uuid >> drop_legacy_temp_table >> proceed_athena_query >> drop_temp_table >> check_s3_object_presence >> rename_s3_objects
            generate_uuid >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table
        return dag


for file in [f for f in glob.glob('**/pipelines/athena_process/**/*.yaml', recursive=True)]:
    dag_config = config_builder(
        default_args=ATHENA_PROCESS_DEFAULT_ARGS, file=file, pipeline_type='athena_process')
    dag_factory = DagFactory(dag_config=dag_config,
                             dag_builder=AthenaProcessDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
