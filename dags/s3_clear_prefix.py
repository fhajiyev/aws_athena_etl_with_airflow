import glob

from airflow import DAG
from operators.s3_clear_prefix_operator import S3ClearPrefixOperator
from operators.athena_get_results_operator import AthenaGetResultsOperator
from operators.check_operator import CheckOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars


S3_CLEAR_PREFIX_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class S3ClearPrefixDagBuilder(DagBuilder, ):
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
            delta_hours = self.pipeline_config['s3']['delta_hours']
            clear_date_temp = f"(execution_date - macros.timedelta(hours={delta_hours}))"

            clear_date = apply_jinja_template(f"{clear_date_temp}.strftime('%Y-%m-%d %H:00:00')")           
            clear_year = apply_jinja_template(f"{clear_date_temp}.strftime('%Y')")
            clear_month = apply_jinja_template(f"{clear_date_temp}.strftime('%m')")
            clear_day = apply_jinja_template(f"{clear_date_temp}.strftime('%d')")
            clear_hour = apply_jinja_template(f"{clear_date_temp}.strftime('%H')")

            bucket=self.pipeline_config['athena']['output_bucket']
            prefix=self.pipeline_config['athena']['output_prefix']

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            get_results_athena_query = AthenaGetResultsOperator(
                clean_path=True,
                bucket=bucket,
                prefix=prefix,
                query=self.pipeline_config['athena']['query'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    start_time=clear_date,
                    end_time=exec_date,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location="s3://{}/{}".format(bucket, prefix),
                workgroup=self.pipeline_config['athena'].get('workgroup', 'primary'),
                sleep_time=5,
                aws_conn_id='aws_athena',
                task_id='get_results_athena_query',
            )

            check_query_result = CheckOperator(
                threshold_value=self.pipeline_config['s3']['delta_hours'],
                comparison_op='le',
                xcom_task_id='get_results_athena_query',
                task_id='check_query_result',
            )

            clear_outdated_snapshot = S3ClearPrefixOperator(
                bucket=self.pipeline_config['s3']['bucket'],
                prefix=self.pipeline_config['s3']['prefix'].format(
                    year=clear_year,
                    month=clear_month,
                    day=clear_day,
                    hour=clear_hour,
                ),
                aws_conn_id='adfit_s3',
                task_id='clear_outdated_snapshot',
            )

            generate_uuid >> get_results_athena_query >> check_query_result >> clear_outdated_snapshot

        return dag


for file in [f for f in glob.glob('**/pipelines/s3_clear_prefix/**/*.yaml', recursive=True)]:
    dag_config = config_builder(
        default_args=S3_CLEAR_PREFIX_DEFAULT_ARGS, file=file, pipeline_type='s3_clear_prefix')
    dag_factory = DagFactory(dag_config=dag_config,
                             dag_builder=S3ClearPrefixDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
