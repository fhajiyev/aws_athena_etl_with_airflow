import glob

from airflow import DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from datetime import timedelta
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.constants import RedshiftLoadType
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from plugins.redshift_plugin.operators.s3_redshift_operator import S3RedshiftOperator
from utils.constants import REDSHIFT_COPY_OPTIONS
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


S3_REDSHIFT_SYNC_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'concurrency': 1,
}


class S3RedshiftSyncDagBuilder(DagBuilder, ):
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

            partition_prefix = self.pipeline_config['s3'].get('partition_prefix', "{{{{ execution_date.strftime('%Y/%m/%d/%H') }}}}").format(
                year="{{ execution_date.strftime('%Y') }}",
                month="{{ execution_date.strftime('%m') }}",
                day="{{ execution_date.strftime('%d') }}",
                hour="{{ execution_date.strftime('%H') }}",
            )

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            reconcile_redshift_table = RedshiftSchemaOperator(
                table_name=self.pipeline_config['redshift']['table_name'],
                create_table=True,
                create_table_syntax=self.pipeline_config['redshift']['create_table_syntax'],

                task_id='reconcile_redshift_table',
            )

            upsert_redshift = S3RedshiftOperator(
                table_name=self.pipeline_config['redshift']['table_name'],
                columns=self.pipeline_config['redshift']['fields'],
                increment_key=self.pipeline_config['redshift']['increment_key'],
                unique_key_list=self.pipeline_config['redshift']['unique_key_list'],
                copy_method=RedshiftLoadType(self.pipeline_config['redshift']['copy_method']),

                redshift_conn_id='redshift',

                s3_bucket=self.pipeline_config['s3']['bucket'],
                s3_prefix='/'.join([
                    self.pipeline_config['s3']['prefix'],
                    partition_prefix,
                ]),
                s3_prefix_dag_run_uuid=False,

                copy_option_list=REDSHIFT_COPY_OPTIONS[self.pipeline_config['redshift']['copy_option']],
                jsonpath_location=self.pipeline_config['redshift'].get('jsonpath_location'),
                timeformat=self.pipeline_config['redshift'].get('timeformat'),

                task_id='upsert_redshift',
            )

            generate_uuid >> reconcile_redshift_table >> upsert_redshift

            if self.pipeline_config.get('delay_seconds'):
                delay_seconds = self.pipeline_config.get('delay_seconds')
                delay_execution = TimeDeltaSensor(
                    delta=timedelta(seconds=delay_seconds),
                    timeout=10,
                    poke_interval=10,
                    retry_delay=timedelta(seconds=delay_seconds),
                    task_id='delay_execution'
                )
                delay_execution >> generate_uuid

        return dag


for file in [f for f in glob.glob('**/pipelines/s3_redshift_sync/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=S3_REDSHIFT_SYNC_DEFAULT_ARGS, file=file, pipeline_type='s3_redshift_sync')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=S3RedshiftSyncDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
