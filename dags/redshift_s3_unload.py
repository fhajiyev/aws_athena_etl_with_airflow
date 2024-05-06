import glob

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import timedelta
from operators.uuid_generator import UUIDGenerator
from operators.s3_clear_prefix_operator import S3ClearPrefixOperator
from plugins.redshift_plugin.operators.redshift_unload_operator import RedshiftUnloadOperator
from utils.constants import REDSHIFT_UNLOAD_OPTIONS
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


log = LoggingMixin().log

REDSHIFT_S3_UNLOAD_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
    # 'sla': timedelta(seconds=3600), https://issues.apache.org/jira/browse/AIRFLOW-5506
}


class RedshiftS3UnloadProcessDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 1),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 1),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        with dag:

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            clean_up_target_s3_prefix = S3ClearPrefixOperator(
                bucket=self.pipeline_config['s3']['bucket'],
                prefix=self.pipeline_config['s3']['prefix'],
                aws_conn_id='adfit_s3',
                task_id='clean_up_target_s3_prefix',
            )

            unload_redshift_data_to_s3 = RedshiftUnloadOperator(
                s3_bucket=self.pipeline_config['s3']['bucket'],
                s3_prefix=self.pipeline_config['s3']['prefix'],
                query=self.pipeline_config['redshift']['select_query'],
                unload_option_list=REDSHIFT_UNLOAD_OPTIONS[self.pipeline_config['redshift'].get('unload_option', 'default_parquet')],
                task_id='unload_redshift_data_to_s3',
            )

            generate_uuid >> clean_up_target_s3_prefix >> unload_redshift_data_to_s3

        return dag


for file in [f for f in glob.glob('**/pipelines/redshift_s3_unload/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=REDSHIFT_S3_UNLOAD_DEFAULT_ARGS, file=file, pipeline_type='redshift_s3_unload')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=RedshiftS3UnloadProcessDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
