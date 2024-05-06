import glob

from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin

from datetime import datetime
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.operators.redshift_spectrum_operator import RedshiftSpectrumOperator
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert

log = LoggingMixin().log

REDSHIFT_SPECTRUM_ADD_PARTITION_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'start_date': datetime(2019, 2, 1, 0),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
}


class RedshiftSpectrumAddPartitionDagBuilder(DagBuilder, ):
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

            execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            slashed_execution_datehour = ""

            if self.pipeline_config['athena']['partition'].get('type') == 'daily':
                slashed_execution_datehour = "{{ execution_date.strftime('%Y/%m/%d') }}"
            elif self.pipeline_config['athena']['partition'].get('type') == 'hourly':
                slashed_execution_datehour = "{{ execution_date.strftime('%Y/%m/%d/%H') }}"
            elif self.pipeline_config['athena']['partition'].get('type') == 'hourly_hive':
                slashed_execution_datehour = "year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/day={{ execution_date.strftime('%d') }}/hour={{ execution_date.strftime('%H') }}"

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            reconcile_spectrum_table = RedshiftSpectrumOperator(
                table_name=self.pipeline_config['athena']['table_name'],

                create_table=True,
                create_table_syntax=self.pipeline_config['athena']['create_table_syntax'],

                add_partition=True,
                partition_name=self.pipeline_config['athena']['partition']['name'],
                partition_value=execution_datehour,
                partition_location='/'.join([
                    self.pipeline_config['athena']['partition']['location'],
                    slashed_execution_datehour
                ]),
                task_id='reconcile_spectrum_table',
            )

            generate_uuid >> reconcile_spectrum_table

        return dag


for file in [f for f in glob.glob('**/pipelines/redshift_spectrum_add_partition/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=REDSHIFT_SPECTRUM_ADD_PARTITION_DEFAULT_ARGS, file=file, pipeline_type='redshift_spectrum_add_partition')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=RedshiftSpectrumAddPartitionDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
