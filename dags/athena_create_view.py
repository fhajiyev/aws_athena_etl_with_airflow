import glob
from airflow import DAG
from datetime import datetime
from functools import partial
from operators.athena_operator import AthenaOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars


ATHENA_VIEW_DEFAULT_ARGS = {
    'owner': 'DataEngineering',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'start_date': datetime(2019, 2, 1, 0),
    'on_failure_callback': partial(task_fail_slack_alert, channel='CGZH7LYAG'),  # Andy test
    'on_retry_callback': partial(task_retry_slack_alert, channel='CGZH7LYAG'),  # Andy test,
    'sla_miss_callback': partial(task_sla_miss_slack_alert, channel='CGZH7LYAG'),  # Andy test
    'concurrency': 1,
}

class AthenaViewDagBuilder(DagBuilder):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            max_active_runs=1,
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        self.__apply_default_values()
        self.__build_templates()

        apply_vars(
            self.pipeline_config,
            ignore_keys=('query',),
            **self.templates,
        )

        manifest_path = "s3://{env}-buzzvil-data-lake/manifest".format(env=self.templates['env'])

        with dag:
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

            reconcile_athena_view = AthenaOperator(
                query=self.pipeline_config['athena']['query'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    **self.templates,
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='reconcile_athena_view',
            )

            generate_uuid >> reconcile_athena_database >> reconcile_athena_view

        return dag

    def __build_templates(self):
        env = apply_jinja_template("var.value.get('server_env', 'prod')")
        execution_date = apply_jinja_template("execution_date.strftime('%Y-%m-%d %H:00:00')")
        next_execution_date = apply_jinja_template("next_execution_date.strftime('%Y-%m-%d %H:00:00')")

        year = apply_jinja_template("execution_date.strftime('%Y')")
        month = apply_jinja_template("execution_date.strftime('%m')")
        day = apply_jinja_template("execution_date.strftime('%d')")
        hour = apply_jinja_template("execution_date.strftime('%H')")

        self.templates = dict(
            env=env,
            execution_date=execution_date,
            next_execution_date=next_execution_date,
            year=year,
            month=month,
            day=day,
            hour=hour,
        )

    def __apply_default_values(self):
        pass


for file in [f for f in glob.glob('**/pipelines/athena_view/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=ATHENA_VIEW_DEFAULT_ARGS, file=file, pipeline_type='athena_view')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=AthenaViewDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
