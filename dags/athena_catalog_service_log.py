import copy
import yaml

from airflow import DAG
from operators.athena_operator import AthenaOperator
from operators.uuid_generator import UUIDGenerator
from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from utils.dag import DagConfig, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars


ATHENA_CATALOG_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 1,
}


class AthenaCatalogDagBuilder(DagBuilder, ):
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

        manifest_path = "s3://{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake/manifest"

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

            reconcile_athena_table = AthenaOperator(
                query=self.pipeline_config['athena']['create_table_syntax'].format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    partition_name=self.pipeline_config['athena']['partition']['name'],
                    location=self.pipeline_config['athena']['location'],
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='reconcile_athena_table',
            )

            partition_athena_table = AthenaOperator(
                query="""
                    ALTER TABLE {database}.{table}
                    ADD IF NOT EXISTS PARTITION ({partition_name} = '{partition_value}')
                    LOCATION '{partition_location}';
                """.format(
                    database=self.pipeline_config['athena']['database'],
                    table=self.pipeline_config['athena']['table'],
                    partition_name=self.pipeline_config['athena']['partition']['name'],
                    partition_value=self.pipeline_config['athena']['partition']['value'],
                    partition_location=self.pipeline_config['athena']['partition']['location']
                ),
                database=self.pipeline_config['athena']['database'],
                output_location=manifest_path,
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id='partition_athena_table',
            )

            generate_uuid >> reconcile_athena_database >> reconcile_athena_table >> partition_athena_table

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
            namespace=self.pipeline_config['namespace'],
            container=self.pipeline_config['container'],

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

def build_pipeline_config(catalog, athena_config):
    table_name = 'l_log'
    if 'table_alias' in catalog:
        table_name = f'l_{catalog["table_alias"]}_log'

    pipeline_key = '{service}_{table_name}'.format(
        service=catalog.get('alias', catalog['namespace']),
        table_name=table_name,
    )

    return {
        'pipeline_key': pipeline_key,
        'pipeline_type': 'athena_catalog',
        'pipeline_dag_configs': {
            'start_date': catalog['start_date'],
            'schedule_interval': '0 * * * *'
        },
        'athena': {
            'database': athena_config['database'],
            'location': athena_config['location'],
            'table': table_name,
            'partition': copy.copy(athena_config['partition']),
            'create_table_syntax': athena_config['create_table_syntax'],
        },
        'namespace': catalog['namespace'],
        'container': catalog.get('container', catalog['namespace']),
    }


with open('pipelines/athena_catalog/service_log.yaml') as f:
    yaml_loaded = yaml.safe_load(f)

    for catalog in yaml_loaded['catalogs']:
        pipeline_config = build_pipeline_config(catalog, yaml_loaded['athena'])

        default_args = copy.copy(ATHENA_CATALOG_DEFAULT_ARGS)
        default_args.update(pipeline_config['pipeline_dag_configs'])

        dag_config = DagConfig(
            default_args=default_args,
            dag_id=f'{pipeline_config["pipeline_type"]}_{pipeline_config["pipeline_key"]}',
            schedule_interval=pipeline_config['pipeline_dag_configs']['schedule_interval'],
            pipeline_config=pipeline_config,
            upstream_dependencies=[],
            downstream_dependencies=[],
            alert_configs=[],
        )
        dag_factory = DagFactory(dag_config=dag_config, dag_builder=AthenaCatalogDagBuilder)
        globals()[dag_factory.get_dag_id()] = dag_factory.create()
