import glob

from airflow import DAG
from operators.athena_operator import AthenaOperator
from operators.athena_get_results_operator import AthenaGetResultsOperator
from operators.check_operator import CheckOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars


ATHENA_UPDATE_VIEW_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class AthenaUpdateViewDagBuilder(DagBuilder, ):
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
            exec_date = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            bucket=self.pipeline_config['athena']['output_bucket']
            prefix=self.pipeline_config['athena']['output_prefix']

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            get_results_athena_query = AthenaGetResultsOperator(
                clean_path=True,
                bucket=bucket,
                prefix=prefix,
                query=self.pipeline_config['athena']['query_count'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    exec_date=exec_date,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location="s3://{}/{}".format(bucket, prefix),
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=5,
                aws_conn_id='aws_athena',
                task_id='get_results_athena_query',
            )

            check_query_result = CheckOperator(
                threshold_value=0,
                comparison_op='eq',
                xcom_task_id='get_results_athena_query',
                task_id='check_query_result',
            )

            update_view_athena_query = AthenaOperator(
                clean_path=True,
                bucket=bucket,
                prefix=prefix,
                query=self.pipeline_config['athena']['query_view'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    exec_date=exec_date,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location="s3://{}/{}".format(bucket, prefix),
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=5,
                aws_conn_id='aws_athena',
                task_id='update_view_athena_query',
            )

            generate_uuid >> get_results_athena_query >> check_query_result >> update_view_athena_query

        return dag


for file in [f for f in glob.glob('**/pipelines/athena_view_update/**/*.yaml', recursive=True)]:
    dag_config = config_builder(
        default_args=ATHENA_UPDATE_VIEW_DEFAULT_ARGS, file=file, pipeline_type='athena_view_update')
    dag_factory = DagFactory(dag_config=dag_config,
                             dag_builder=AthenaUpdateViewDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
