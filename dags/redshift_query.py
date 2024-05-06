import glob

from airflow import DAG
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert


REDSHIFT_QUERY_DEFAULT_ARGS = {
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
            concurrency=self.pipeline_config.get('pipeline_dag_configs', {}).get('concurrency', 1),
            max_active_runs=self.pipeline_config.get('pipeline_dag_configs', {}).get('max_active_runs', 1),
            catchup=True,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )

        self.__apply_default_values()

        with dag:
            RedshiftOperator(
                sql="""
                    {% raw %}
                    {param[query]}
                    {% endraw %}
                    """,
                redshift_conn_id=self.pipeline_config['redshift']['conn_id'],
                param_dict=dict({
                    'query': self.pipeline_config['redshift']['query'],
                }),
                task_id='run_redshift_query',
            )

        return dag

    def __apply_default_values(self):
        self.pipeline_config['redshift']['conn_id'] = self.pipeline_config['redshift'].get('conn_id', 'redshift')


for file in [f for f in glob.glob('**/pipelines/redshift_query/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=REDSHIFT_QUERY_DEFAULT_ARGS, file=file, pipeline_type='redshift_query')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=RedshiftQueryDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
