import glob

from airflow import DAG
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from operators.athena_operator import AthenaOperator
from operators.athena_deduplicate_operator import AthenaDeduplicateOperator
from operators.s3_clear_prefix_operator import S3ClearPrefixOperator
from operators.s3_rename_objects_operator import S3RenameObjectsOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars


ATHENA_PROCESS_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 3,
}

class AthenaDeduplicateDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 3),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 3),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        self.__apply_default_values()
        self.__build_templates()

        apply_vars(
            self.pipeline_config,
            ignore_keys=('create_table_syntax',),
            **self.templates,
        )

        manifest_path = "s3://{env}-buzzvil-data-lake/manifest".format(env=self.templates['env'])
        temp_table = '_'.join([self.pipeline_config['athena']['table'], 'temp', self.templates['year'], self.templates['month'], self.templates['day'], self.templates['hour']])

        with dag:
            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            drop_legacy_temp_table = AthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=self.pipeline_config['athena']['database'],
                    temp_table=temp_table,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                workgroup=self.pipeline_config['athena']['workgroup'],
                task_id='drop_legacy_temp_table',
            )

            drop_legacy_s3_object = S3ClearPrefixOperator(
                bucket=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'],
                aws_conn_id='adfit_s3',
                task_id='drop_legacy_s3_object',
            )

            proceed_athena_dedup_query = AthenaDeduplicateOperator(
                create_start_time=self.templates['create_start_date'],
                create_end_time=self.templates['create_end_date'],
                end_time=self.templates['next_execution_date'],
                database=self.pipeline_config['athena']['database'],
                original_table=self.pipeline_config['athena']['deduplication']['original_table'],
                temp_table=temp_table,
                output_bucket=self.pipeline_config['athena']['output_bucket'],
                output_prefix=self.pipeline_config['athena']['output_prefix'],
                fields=self.pipeline_config['athena']['deduplication']['fields'],
                dedup_type=self.pipeline_config['athena']['deduplication']['type'],
                unique_fields=self.pipeline_config['athena']['deduplication']['unique_fields'],
                updated_field=self.pipeline_config['athena']['deduplication']['updated_field'],
                updated_values=self.pipeline_config['athena']['deduplication'].get('updated_values', None),
                bucketed_by=self.pipeline_config['athena']['deduplication'].get('bucketed_by', None),
                bucket_count=self.pipeline_config['athena']['deduplication'].get('bucket_count', None),

                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                workgroup=self.pipeline_config['athena']['workgroup'],
                task_id='proceed_athena_dedup_query',
            )

            drop_temp_table = AthenaOperator(
                query='DROP TABLE IF EXISTS {database}.{temp_table};'.format(
                    database=self.pipeline_config['athena']['database'],
                    temp_table=temp_table,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='drop_temp_table',
            )

            check_s3_object_presence = S3PrefixSensor(
                bucket_name=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'],
                poke_interval=1,
                timeout=10,
                soft_fail=self.pipeline_config['athena']['skip_on_empty_query_result'],
                aws_conn_id='adfit_s3',
                task_id='check_s3_object_presence',
            )

            rename_s3_objects = S3RenameObjectsOperator(
                bucket=self.pipeline_config['athena']['output_bucket'],
                prefix=self.pipeline_config['athena']['output_prefix'],
                file_key='{file_key}-{year}-{month}-{day}-{hour}'.format(
                    file_key=self.pipeline_config['athena']['file_key'],
                    year=self.templates['year'],
                    month=self.templates['month'],
                    day=self.templates['day'],
                    hour=self.templates['hour'],
                ),
                file_extension=self.pipeline_config['athena']['file_extension'],
                task_id='rename_s3_objects',
            )

            reconcile_athena_database = AthenaOperator(
                query='CREATE DATABASE IF NOT EXISTS {database};'.format(
                    database=self.pipeline_config['athena']['database']
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                workgroup=self.pipeline_config['athena']['workgroup'],
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
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                workgroup=self.pipeline_config['athena']['workgroup'],
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
                    partition_value=self.templates['create_start_date'],
                    partition_location='/'.join([
                        self.pipeline_config['athena']['location'],
                        self.pipeline_config['athena']['partition']['subdir'],
                    ]),
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='partition_athena_table',
            )

            generate_uuid >> drop_legacy_temp_table >> proceed_athena_dedup_query
            generate_uuid >> drop_legacy_s3_object >> proceed_athena_dedup_query
            proceed_athena_dedup_query >> drop_temp_table >> check_s3_object_presence >> rename_s3_objects

            generate_uuid >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table
        return dag

    def __build_templates(self):
        env = "{{ var.value.get('server_env', 'prod') }}"
        execution_date = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        next_execution_date = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"

        scan_days = self.pipeline_config['athena']['deduplication']['scan_days']
        create_start_date = f"(execution_date - macros.timedelta(days={scan_days}))"
        create_end_date = f"(next_execution_date - macros.timedelta(days={scan_days}))"

        year = apply_jinja_template(f"{create_start_date}.strftime('%Y')")
        month = apply_jinja_template(f"{create_start_date}.strftime('%m')")
        day = apply_jinja_template(f"{create_start_date}.strftime('%d')")
        hour = apply_jinja_template(f"{create_start_date}.strftime('%H')")

        create_start_date = apply_jinja_template(f"{create_start_date}.strftime('%Y-%m-%d %H:00:00')")
        create_end_date = apply_jinja_template(f"{create_end_date}.strftime('%Y-%m-%d %H:00:00')")

        self.templates = dict(
            env=env,
            execution_date=execution_date,
            next_execution_date=next_execution_date,
            create_start_date=create_start_date,
            create_end_date=create_end_date,
            year=year,
            month=month,
            day=day,
            hour=hour,
        )

    def __apply_default_values(self):
        self.pipeline_config['athena']['file_extension'] = self.pipeline_config['athena'].get('file_extension', 'parquet')
        self.pipeline_config['athena']['partition']['key'] = self.pipeline_config['athena']['partition'].get('key', 'partition_timestamp')
        self.pipeline_config['athena']['partition']['subdir'] = self.pipeline_config['athena']['partition'].get('subdir', 'year={year}/month={month}/day={day}/hour={hour}')
        self.pipeline_config['athena']['skip_on_empty_query_result'] = self.pipeline_config['athena'].get('skip_on_empty_query_result', True)
        self.pipeline_config['athena']['workgroup'] = self.pipeline_config['athena'].get('workgroup', 'primary')

for file in [f for f in glob.glob('**/pipelines/athena_deduplicate/**/*.yaml', recursive=True)]:
    dag_config = config_builder(
        default_args=ATHENA_PROCESS_DEFAULT_ARGS, file=file, pipeline_type='athena_deduplicate')
    dag_factory = DagFactory(dag_config=dag_config,
                             dag_builder=AthenaDeduplicateDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
