from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.redshift_plugin.hooks.redshift_hook import RedshiftHook
from utils.decorators import get_dag_run_uuid


class RedshiftSchemaOperator(BaseOperator):
    """
    Operator to reconcile Redshift schema before operating on it

    :param candidate_table: name of the redshift table to be migrated
    :type candidate_table: string

    :param s3_location: url of the s3 location (up to prefix level) to store intermediary data
    :type string:
    """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            table_name,

            create_table=False,
            create_table_syntax='',
            drop_existing_table=False,

            retrieve_last_increment_value=False,
            increment_key='',
            increment_key_type='',

            redshift_conn_id='redshift',
            database='buzzad',
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.database = database

        self.create_table = create_table
        self.create_table_syntax = create_table_syntax
        self.drop_existing_table = drop_existing_table

        self.retrieve_last_increment_value = retrieve_last_increment_value
        self.increment_key = increment_key
        self.increment_key_type = increment_key_type
        self.last_increment_value = None

        self.dag_run_uuid = None

    def _run_query(self, query_string, fetch=False):
        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id, schema=self.database)

        result = None
        if fetch is True:
            result = self.hook.dictfetchall(query_string)
        else:
            self.hook.run(sql=query_string,)
        return result

    def _create_table(self, ):
        self._run_query(
            query_string=self.create_table_syntax.format(
                table_name=self.table_name,
            ),
            fetch=False,
        )

    def _drop_table_if_exists(self, ):
        self._run_query(
            query_string="""
                DROP TABLE IF EXISTS {table_name};
            """.format(table_name=self.table_name),
            fetch=False,
        )

    def _retrieve_last_increment_value(self, ):
        RETRIEVE_LAST_INCREMENT_VALUE = """
            SELECT
                {increment_key} as increment_key
            FROM
                {table_name}
            ORDER BY
                {increment_key} DESC
            LIMIT 1
        """
        formatted_string = RETRIEVE_LAST_INCREMENT_VALUE.format(
            increment_key=self.increment_key,
            table_name=self.table_name,
        )
        res = self._run_query(query_string=formatted_string, fetch=True)

        if len(res) > 0:
            if self.increment_key_type == 'timestamp':  # datetime -> String
                self.last_increment_value = str(res[0]['increment_key'])
            else:
                self.last_increment_value = res[0]['increment_key']

        else:
            self.last_increment_value = None

    @get_dag_run_uuid
    def execute(self, context):
        task_instance = context['task_instance']

        if self.drop_existing_table is True:
            self._drop_table_if_exists()

        if self.create_table is True:
            self._create_table()

        if self.retrieve_last_increment_value is True:
            self._retrieve_last_increment_value()

            self.log.info(
                'last_increment_value is {0}'.format(
                    self.last_increment_value
                )
            )

            task_instance.xcom_push(
                key='last_increment_value',
                value=dict({'last_increment_value': self.last_increment_value})
            )
