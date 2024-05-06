import glob

from airflow import DAG

from operators.uuid_generator import UUIDGenerator
from operators.s3_to_mysql import S3ToMySqlOperator
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


S3_MYSQL_LOAD_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class S3MysqlLoadDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 3),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 3),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )
        with dag:
            year = "{{ execution_date.strftime('%Y') }}"
            month = "{{ execution_date.strftime('%m') }}"
            day = "{{ execution_date.strftime('%d') }}"
            hour = "{{ execution_date.strftime('%H') }}"

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            upsert_mysql = S3ToMySqlOperator(
                task_id='upsert_mysql',
                bucket=self.pipeline_config['s3']['bucket'],
                prefix=self.pipeline_config['s3']['prefix'].format(
                        year=year,
                        month=month,
                        day=day,
                        hour=hour,
                    ),
                mysql_table=self.pipeline_config['mysql']['table'],
                target_fields=self.pipeline_config['mysql']['target_fields'],
                chunk_size=self.pipeline_config['mysql']['chunk_size'],
                replace=self.pipeline_config['mysql']['replace'],
                aws_conn_id='aws_athena',
                mysql_conn_id='adrecommender_mysql',
            )
            generate_uuid >> upsert_mysql

        return dag


for file in [f for f in glob.glob('**/pipelines/s3_mysql_load/**/*.yaml', recursive=True)]:
    dag_config = config_builder(
        default_args=S3_MYSQL_LOAD_DEFAULT_ARGS, file=file, pipeline_type='s3_mysql_load')
    dag_factory = DagFactory(dag_config=dag_config,
                             dag_builder=S3MysqlLoadDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
