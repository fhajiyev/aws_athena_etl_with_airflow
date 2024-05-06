import glob

from airflow import DAG
from operators.athena_operator import AthenaOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


ATHENA_CATALOG_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class AthenaCatalogDagBuilder(DagBuilder, ):
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
            year = "{{ execution_date.strftime('%Y') }}"
            month = "{{ execution_date.strftime('%m') }}"
            day = "{{ execution_date.strftime('%d') }}"
            hour = "{{ execution_date.strftime('%H') }}"

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            reconcile_athena_database = AthenaOperator(
                query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                    database=self.pipeline_config['athena']['database']
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                aws_conn_id='aws_athena',
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
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
                    )
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='partition_athena_table',
            )

            generate_uuid >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table

        return dag


for file in [f for f in glob.glob('**/pipelines/athena_catalog/*/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=ATHENA_CATALOG_DEFAULT_ARGS, file=file, pipeline_type='athena_catalog')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=AthenaCatalogDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
