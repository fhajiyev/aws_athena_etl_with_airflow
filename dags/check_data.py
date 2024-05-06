import glob
import operator

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python_operator import PythonOperator

from plugins.athena_plugin.constants import DEFAULT_GETQUERYRESULTS_POKE_INTERVAL
from operators.athena_operator import AthenaOperator
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from utils.dag import config_builder, DagFactory, DagBuilder
from utils.slack import task_fail_slack_alert, task_retry_slack_alert, task_sla_miss_slack_alert
from utils.template import apply_jinja_template, apply_vars

from airflow.utils.log.logging_mixin import LoggingMixin


logger = LoggingMixin().log


CHECK_DATA_DEFAULT_ARGS = {
    'owner': 'datavil',
    'depends_on_past': False,
    'backfill': True,
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'sla_miss_callback': task_sla_miss_slack_alert,
    'concurrency': 3,
}


class DataSourceOperatorFactory():
    """
    DataSourceOperatorFactory
    """
    def __init__(self, task_id, templates, data_source, query=None):
        self.task_id = task_id
        self.templates = templates
        self.data_source = data_source
        self.query = query

    def create(self, ):
        if self.data_source['type'] == 'numeric_zero':
            return PythonOperator(
                python_callable=lambda **kwargs: kwargs['ti'].xcom_push(key='query_result', value=[{'result': 0}]),
                provide_context=True,
                task_id=self.task_id,
            )
        elif self.data_source['type'] == 'redshift':
            # requireds fields: conn_id
            return RedshiftOperator(
                sql=self.query,
                redshift_conn_id=self.data_source['conn_id'],
                fetch=True,
                push_xcom=True,
                task_id=self.task_id,
            )
        elif self.data_source['type'] == 'athena':
            # required fields: database
            return AthenaOperator(
                query=self.query,
                database=self.data_source['database'],
                output_location="s3://{env}-buzzvil-data-lake/manifest".format(**self.templates),
                workgroup=self.data_source['workgroup'],
                sleep_time=DEFAULT_GETQUERYRESULTS_POKE_INTERVAL,
                aws_conn_id='aws_athena',
                task_id=self.task_id,
                push_xcom=True,
            )

        else: # TODO : Implement ('athena', 'mysql')
            raise NotImplementedError(f'DataSourceOperator for {self.data_source_type} was not implemented')


OPERATORS = {
    'eq': { 'operator': operator.eq, 'symbol': '=' },
    'ge': { 'operator': operator.ge, 'symbol': '>=' },
    'gt': { 'operator': operator.gt, 'symbol': '>' },
    'le': { 'operator': operator.le, 'symbol': '<=' },
    'lt': { 'operator': operator.lt, 'symbol': '<' },
}


def __pull_xcom_data(ti, task_id):
    # 'data is None error'는 upstream operator에서 발생하도록 구현해야됨.
    # compare_data operator는 혹시 구현되지않았을때를 미리 막아두고자 raise함.
    # 해당 에러가 날경우 upstream operator를 수정해야한다
    xcom_data = ti.xcom_pull(key='query_result', task_ids=task_id)
    if len(xcom_data) != 1:
        raise AirflowException(f'query_result of {task_id} has {len(xcom_data)} rows instead of 1 row.')
    elif xcom_data[0].get('result') is None:
        raise AirflowException(f'query_result of {task_id} has no "result" column')

    return xcom_data[0]['result']

# TODO move it into operators
def evaluate_data(
    ti,
    augend_multiplier,
    addend_multiplier,
    op,
    threshold,
    augend_default,
    addend_default,
    **kwargs
):
    """
    Evaluates ax1 + bx2 (op) threshold.
    Note that the results from each data source must be a single, numeric value.
    :param kwargs:
    :return:
    """

    augend_data = __pull_xcom_data(ti, 'get_augend_data')
    addend_data = __pull_xcom_data(ti, 'get_addend_data')

    lhs = float(augend_data) * float(augend_multiplier) + float(addend_data) * float(addend_multiplier)
    threshold = float(threshold)

    if op not in OPERATORS:
        raise AttributeError(f"Operator : {op} is not supported. Choose one from {OPERATORS.keys()}")

    operator = OPERATORS[op]['operator']
    symbol = OPERATORS[op]['symbol']

    logger.info(f"Comparing {float(augend_data)} * {float(augend_multiplier)} + {float(addend_data)} * {float(addend_multiplier)} {symbol} {threshold}")

    if not operator(lhs, threshold):
        raise AirflowFailException(f"{lhs}  {symbol}  {threshold} did not evaluate TRUE")

    return operator(lhs, threshold)


class CheckDataDagBuilder(DagBuilder):
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
            ignore_keys={},
            **self.templates,
        )

        with dag:
            generate_uuid = UUIDGenerator(
                task_id='generate_uuid',
            )

            get_augend_data = DataSourceOperatorFactory(
                task_id='get_augend_data',
                templates=self.templates,
                data_source=self.pipeline_config['augend']['data_source'],
                query=self.pipeline_config['augend']['query'],
            ).create()

            get_addend_data = DataSourceOperatorFactory(
                task_id='get_addend_data',
                templates=self.templates,
                data_source=self.pipeline_config['addend']['data_source'],
                query=self.pipeline_config['addend']['query'],
            ).create()

            compare_data = PythonOperator(
                python_callable=evaluate_data,
                op_kwargs=dict({
                    'augend_default': self.pipeline_config['augend']['default'],
                    'addend_default': self.pipeline_config['addend']['default'],
                    'augend_multiplier': self.pipeline_config['augend']['multiplier'],
                    'addend_multiplier': self.pipeline_config['addend']['multiplier'],
                    'op': self.pipeline_config['comparison']['operator'],
                    'threshold': self.pipeline_config['comparison']['threshold'],
                }),
                provide_context=True,

                task_id='compare_data',
            )

            generate_uuid >> [get_augend_data, get_addend_data] >> compare_data

        return dag

    def __build_templates(self):
        env = "{{ var.value.get('server_env', 'prod') }}"
        execution_date = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        next_execution_date = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        start_time = "{{ execution_date.strftime('%Y-%m-%d %H:00:00') }}"
        end_time = "{{ next_execution_date.strftime('%Y-%m-%d %H:00:00') }}"

        self.templates = dict(
            env=env,
            execution_date=execution_date,
            next_execution_date=next_execution_date,
            start_time=start_time,
            end_time=end_time,
        )

    def __apply_default_values(self):
        if 'augend' not in self.pipeline_config.keys():
            raise AirflowException("Configurations for augend are mandatory")

        self.pipeline_config['augend']['multiplier'] = self.pipeline_config['augend'].get('multiplier', 1)
        self.pipeline_config['augend']['default'] = self.pipeline_config['augend'].get('default', None)

        self.pipeline_config['addend'] = self.pipeline_config.get(
            'addend',
            {
                'data_source': {'type': 'numeric_zero'},
                'query': None,
                'multiplier': 1,
                'default': None,
            },
        )
        self.pipeline_config['addend']['query'] = self.pipeline_config['addend'].get('query', None)
        self.pipeline_config['addend']['multiplier'] = self.pipeline_config['addend'].get('multiplier', 1)
        self.pipeline_config['addend']['default'] = self.pipeline_config['addend'].get('default', None)
        

for file in [f for f in glob.glob('**/pipelines/check_data/**/*.yaml', recursive=True)]:
    dag_config = config_builder(default_args=CHECK_DATA_DEFAULT_ARGS, file=file, pipeline_type='check_data')
    dag_factory = DagFactory(dag_config=dag_config, dag_builder=CheckDataDagBuilder)
    globals()[dag_factory.get_dag_id()] = dag_factory.create()
