import glob

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from plugins.athena_plugin.operators.athena_operator import AthenaOperator
from plugins.redshift_plugin.constants import RedshiftLoadType
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from plugins.redshift_plugin.operators.redshift_spectrum_operator import RedshiftSpectrumOperator
from plugins.redshift_plugin.operators.s3_redshift_operator import S3RedshiftOperator
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.constants import REDSHIFT_COPY_OPTIONS
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert

ATHENA_REDSHIFT_SYNC_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'on_sla_miss_callback': task_sla_miss_slack_alert,
}

class AthenaRedshiftSyncDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 5),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 5),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        with dag:

            execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            next_execution_datehour = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            partition_prefix = ''
            hive_hourly_prefix = "year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/day={{ execution_date.strftime('%d') }}/hour={{ execution_date.strftime('%H') }}"

            if self.pipeline_config['athena']['partition']['type'] == 'daily':
                partition_prefix = "{{ execution_date.strftime('%Y/%m/%d') }}"
            elif self.pipeline_config['athena']['partition']['type'] == 'hourly':
                partition_prefix = "{{ execution_date.strftime('%Y/%m/%d/%H') }}"
            elif self.pipeline_config['athena']['partition']['type'] == 'hourly_hive':
                partition_prefix = hive_hourly_prefix
            else:
                raise AirflowFailException("athena.partition.type should be one of 'daily', 'hourly', or 'hourly_hive'")

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            # Note : In order to use Spectrum compatible external tables, create reconcile athena table using redshift spectrum if applicable.
            reconcile_athena_table = RedshiftSpectrumOperator(
                table_name=self.pipeline_config['athena']['table_name'],

                create_table=True,
                create_table_syntax=self.pipeline_config['athena']['create_table_syntax'],

                add_partition=True,
                partition_name=self.pipeline_config['athena']['partition']['name'],
                partition_value=execution_datehour,
                partition_location='/'.join([
                    self.pipeline_config['athena']['partition']['location'],
                    partition_prefix,
                ]),

                task_id='reconcile_athena_table',
            )

            reconcile_redshift_table = RedshiftSchemaOperator(
                table_name=self.pipeline_config['redshift']['table_name'],
                create_table=True,
                create_table_syntax=self.pipeline_config['redshift']['create_table_syntax'],

                task_id='reconcile_redshift_table',
            )

            athena_preprocess_table = AthenaOperator(
                query=self.pipeline_config['athena']['process_query'].format(
                    start_time=execution_datehour,
                    end_time=next_execution_datehour,
                ),
                database=Variable.get(key='athena_database'),
                output_bucket=self.pipeline_config['athena']['output_bucket'],
                output_prefix='/'.join([
                    self.pipeline_config['athena']['output_prefix'],
                    hive_hourly_prefix,
                ]),
                file_key=self.pipeline_config['athena']['file_key'],
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',

                task_id='athena_preprocess_table',
            )

            upsert_redshift = S3RedshiftOperator(
                table_name=self.pipeline_config['redshift']['table_name'],
                columns=self.pipeline_config['redshift']['fields'],
                increment_key=self.pipeline_config['redshift'].get('increment_key', {}),
                unique_key_list=self.pipeline_config['redshift'].get('unique_key_list', {}),
                deduplicate_key_list=self.pipeline_config['redshift'].get('deduplicate_key_list', {}),
                copy_method=RedshiftLoadType(self.pipeline_config['redshift']['copy_method']),

                redshift_conn_id='redshift',

                s3_bucket=self.pipeline_config['athena']['output_bucket'],
                s3_prefix='/'.join([
                    self.pipeline_config['athena']['output_prefix'],
                    hive_hourly_prefix,
                ]),

                copy_option_list=REDSHIFT_COPY_OPTIONS['athena_csv'],

                task_id='upsert_redshift',
            )

            generate_uuid >> reconcile_athena_table >> athena_preprocess_table
            generate_uuid >> reconcile_redshift_table >> athena_preprocess_table

            athena_preprocess_table >> upsert_redshift

        return dag


for file in [f for f in glob.glob('**/pipelines/athena_redshift/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=ATHENA_REDSHIFT_SYNC_DEFAULT_ARGS, file=file, pipeline_type='athena_redshift_sync')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=AthenaRedshiftSyncDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
