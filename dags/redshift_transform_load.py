import glob

from airflow import DAG
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from utils.constants import XcomParam
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


REDSHIFT_TRANSFORM_LOAD_QUERY_DEFAULT_ARGS = {
    'owner': 'devop',
    'depends_on_past': True,
    'backfill': True,
    'retries': 5,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'concurrency': 1,
}


class RedshiftQueryDagBuilder(DagBuilder, ):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 5),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 1),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        self.__apply_default_values()

        if 'deduplicate_key_list' not in self.pipeline_config['redshift'].keys():
            self.pipeline_config['redshift']['deduplicate_key_list'] = self.pipeline_config['redshift']['unique_key_list']


        with dag:

            execution_datehour = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
            next_execution_datehour = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"

            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            reconcile_redshift_table = RedshiftSchemaOperator(
                table_name=self.pipeline_config['redshift']['table_name'],
                create_table=True,
                create_table_syntax=self.pipeline_config['redshift']['create_table_syntax'],
                retrieve_last_increment_value=False,
                redshift_conn_id=self.pipeline_config['redshift']['conn_id'],
                database='buzzad',

                task_id='reconcile_redshift_table',
            )

            transform_reload_data = RedshiftOperator(
                sql="""
                    {% raw %}
                    BEGIN;
                    CREATE TABLE IF NOT EXISTS {param[table_name]}_staging_{{param[dag_run_uuid]}} (LIKE {param[table_name]});

                    INSERT INTO {param[table_name]}_staging_{{param[dag_run_uuid]}}
                    (
                        {param[transform_subquery]}
                    );

                    {param[delete_query]};

                    DELETE FROM {param[table_name]}
                    USING {param[table_name]}_staging_{{param[dag_run_uuid]}}
                    WHERE {param[unique_condition]};

                    INSERT INTO {param[table_name]}
                    (
                        SELECT
                            {param[columns]}
                        FROM
                            (
                            SELECT
                                *, ROW_NUMBER() OVER (PARTITION BY {param[deduplicate_by]} ORDER BY {param[increment_key]} DESC) AS rownum
                            FROM
                                {param[table_name]}_staging_{{param[dag_run_uuid]}}
                            )
                        WHERE rownum = 1
                    );
                    DROP TABLE {param[table_name]}_staging_{{param[dag_run_uuid]}};
                    END;
                    {% endraw %}
                    """,
                redshift_conn_id=self.pipeline_config['redshift']['conn_id'],
                xcom_params=[
                    XcomParam(xcom_source_task_id='generate_uuid', xcom_param_key='dag_run_uuid'),
                ],
                param_dict=dict({
                    'table_name': self.pipeline_config['redshift']['table_name'],
                    'columns': ', '.join(self.pipeline_config['redshift']['fields']),
                    'deduplicate_by': ', '.join(self.pipeline_config['redshift']['deduplicate_key_list']),
                    'delete_query': self.pipeline_config['transform']['delete_query'].format(
                        start_time=execution_datehour,
                        end_time=next_execution_datehour,
                    ),
                    'increment_key': self.pipeline_config['redshift']['increment_key'],
                    'transform_subquery': self.pipeline_config['transform']['select_query'].format(
                        start_time=execution_datehour,
                        end_time=next_execution_datehour,
                    ),
                    'unique_condition': ' AND '.join(
                        '{table_name}.{key} = {table_name}_staging_{{param[dag_run_uuid]}}.{key}'.format(
                            table_name=self.pipeline_config['redshift']['table_name'],
                            key=key,
                        ) for key in self.pipeline_config['redshift']['unique_key_list']
                    ),
                }),

                task_id='transform_reload_data',
            )

            generate_uuid >> reconcile_redshift_table  >> transform_reload_data

        return dag

    def __apply_default_values(self):
        self.pipeline_config['redshift']['conn_id'] = self.pipeline_config['redshift'].get('conn_id', 'redshift')


for file in [f for f in glob.glob('**/pipelines/redshift_transform_load/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=REDSHIFT_TRANSFORM_LOAD_QUERY_DEFAULT_ARGS, file=file, pipeline_type='redshift_transform_load')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=RedshiftQueryDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
