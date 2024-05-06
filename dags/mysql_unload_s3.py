import glob

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from operators.athena_operator import AthenaOperator
from operators.mysql_s3_operator import MySqlS3Operator
from operators.uuid_generator import UUIDGenerator

from utils.constants import DEFAULT_MYSQL_BATCH_SIZE
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


log = LoggingMixin().log

MYSQL_DATA_LAKE_LOAD_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class MySqlUnloadS3DagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            max_active_runs=10,
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        manifest_path = "s3://{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake/manifest"

        with dag:
            execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            year = "{{ execution_date.strftime('%Y') }}"
            month = "{{ execution_date.strftime('%m') }}"
            day = "{{ execution_date.strftime('%d') }}"
            hour = "{{ execution_date.strftime('%H') }}"

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            unload_mysql_data_to_s3 = MySqlS3Operator(
                mysql_conn_id=self.pipeline_config['mysql']['conn_id'],
                mysql_table_name=self.pipeline_config['mysql']['table_name'],
                mysql_columns=self.pipeline_config['mysql']['fields'],
                increment_key=self.pipeline_config['mysql']['increment_key'],
                increment_key_type=self.pipeline_config['mysql']['increment_key_type'],
                s3_conn_id='adfit_s3',
                s3_bucket=self.pipeline_config['s3']['bucket'],
                s3_prefix=self.pipeline_config['s3']['prefix'].format(
                    year=year,
                    month=month,
                    day=day,
                    hour=hour
                ),
                batch_period=self.pipeline_config['mysql'].get('batch_period', 'day'),
                batch_size=self.pipeline_config['mysql'].get('batch_size', DEFAULT_MYSQL_BATCH_SIZE),
                file_key=self.pipeline_config['s3']['file_key'],
                data_format=self.pipeline_config['s3']['data_format'],
                task_id='unload_mysql_data_to_s3',
            )

            generate_uuid >> unload_mysql_data_to_s3

            if self.pipeline_config.get('athena'):
                reconcile_athena_database = AthenaOperator(
                    query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                        database=self.pipeline_config['athena']['database']
                    ),
                    database=self.pipeline_config['athena']['database'],
                    output_location=manifest_path,
                    aws_conn_id='aws_athena',
                    task_id='reconcile_athena_database',
                )

                reconcile_athena_table = AthenaOperator(
                    query=self.pipeline_config['athena']['create_table_syntax'].format(
                        database=self.pipeline_config['athena']['database'],
                        table=self.pipeline_config['athena']['table'],
                        partition_key=self.pipeline_config['athena']['partition']['key'],
                        location=self.pipeline_config['athena']['location'],
                    ),
                    database=self.pipeline_config['athena']['database'],
                    output_location=manifest_path,
                    aws_conn_id='aws_athena',
                    task_id='reconcile_athena_table',
                )

                partition_athena_table = AthenaOperator(
                    query="""
                        ALTER TABLE {database}.{table}
                        ADD IF NOT EXISTS PARTITION ({partition_key} = '{partition_value}')
                        LOCATION '{partition_location}';
                    """.format(
                        database=self.pipeline_config['athena']['database'],
                        table=self.pipeline_config['athena']['table'],
                        partition_key=self.pipeline_config['athena']['partition']['key'],
                        partition_value=self.pipeline_config['athena']['partition'].get('value', execution_datehour),
                        partition_location='/'.join([
                            self.pipeline_config['athena']['location'],
                            self.pipeline_config['athena']['partition'].get('subdir', f'year={year}/month={month}/day={day}/hour={hour}'),
                        ]),
                    ),
                    database=self.pipeline_config['athena']['database'],
                    output_location=manifest_path,
                    aws_conn_id='aws_athena',
                    task_id='partition_athena_table',
                )

                unload_mysql_data_to_s3 >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table

        return dag


for file in [f for f in glob.glob('**/pipelines/mysql_unload_s3/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=MYSQL_DATA_LAKE_LOAD_DEFAULT_ARGS, file=file, pipeline_type='mysql_unload_s3')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=MySqlUnloadS3DagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
