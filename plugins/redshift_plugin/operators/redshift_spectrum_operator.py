from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2 import InternalError as InternalError
from plugins.redshift_plugin.hooks.redshift_hook import RedshiftHook
from utils.decorators import get_dag_run_uuid


class RedshiftSpectrumOperator(BaseOperator):
    """
    Operator to reconcile Redshift Spectrum schema before operating on it

    :param candidate_table: name of the redshift table to be migrated
    :type candidate_table: string

    :param s3_location: url of the s3 location (up to prefix level) to store intermediary data
    :type string:

    NOTE : Currently, Redshift Spectrum shares data catalog with Athena and cannot run CREATE IF NOT EXISTS.
           Thus creation of the table should be done outside of code at the moment.
    """

    template_fields = ('partition_value', 'partition_location')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            table_name,
            partition_name,
            partition_value,
            partition_location,

            create_table=False,
            create_table_syntax=None,
            drop_existing_table=False,

            add_partition=False,

            redshift_conn_id='redshift',
            database='buzzad',
            spectrum_database='spectrum',
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.database = database
        self.spectrum_database = spectrum_database

        self.create_table = create_table
        self.create_table_syntax = create_table_syntax
        self.drop_existing_table = drop_existing_table

        self.add_partition = add_partition
        self.partition_name = partition_name
        self.partition_value = partition_value
        self.partition_location = partition_location

        self.dag_run_uuid = None
        self.param_dict = dict()

    def _run_query(self, query_string, param_dict=dict()):
        if bool(param_dict) is True:
            query_string = query_string.format(param=param_dict)

        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id, schema=self.database)
        self.hook.run(sql=query_string, autocommit=True)  # Queries on external tables should have isolation level = AUTOCOMMIT

    def _create_table(self, ):
        try:
            self._run_query(query_string=self.create_table_syntax)
        except InternalError as e:
            self.log.info(str(e))
            if "Table already exists" in str(e):
                self.log.info("The table already exists, skipping table creation")
            else:
                raise Exception(e)

    def _drop_table_if_exists(self, ):
        self._run_query(
            query_string="""
                DROP TABLE IF EXISTS {table_name};
            """.format(table_name=self.table_name),
        )

    def _add_partition(self, ):
        ADD_PARTITION_QUERY = """
            ALTER TABLE {param[database]}.{param[table_name]}
            ADD IF NOT EXISTS PARTITION ({param[partition_name]} = '{param[partition_value]}')
            LOCATION '{param[partition_location]}'
        """
        self.param_dict = dict({
            'database': self.spectrum_database,
            'table_name': self.table_name,
            'partition_name': self.partition_name,
            'partition_value': self.partition_value,
            'partition_location': self.partition_location,
        })
        self._run_query(query_string=ADD_PARTITION_QUERY, param_dict=self.param_dict)

    @get_dag_run_uuid
    def execute(self, context):

        if self.drop_existing_table is True:
            self._drop_table_if_exists()

        if self.create_table is True:
            self._create_table()

        if self.add_partition is True:
            self._add_partition()
