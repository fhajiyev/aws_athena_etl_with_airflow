from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
from plugins.redshift_plugin.hooks.redshift_hook import RedshiftHook
from utils.decorators import get_dag_run_uuid


class RedshiftUnloadOperator(BaseOperator):
    """
    Unloads a table in Redshift to S3

    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string

    :param database: name of the redshift database
    :type database: string

    :param redshift_candidate_table: name of the redshift table to be migrated
    :type redshift_candidate_table: string

    :param s3_location: url of the s3 location (up to prefix level) to store intermediary data
    :type string:
    """

    template_fields = ('s3_bucket', 's3_prefix', 'query',)

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_prefix,

            query=None,
            table_name=None,

            unload_option_list=None,
            migration=False,

            redshift_conn_id='redshift',
            database=None,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.database = database
        self.redshift_conn_id = redshift_conn_id

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix

        self.unload_option_list = unload_option_list

        self.query = query
        self.table_name = table_name
        self.migration = migration

        self.migration_id = None
        self.increment_key = None
        self.unique_key_list = None

        self.dag_run_uuid = None

    def _run_query(self, query_string, fetch=False):
        self.log.info('Executing: %s', query_string)
        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id, schema=self.database)
        if fetch is True:
            result = self.hook.dictfetchall(query_string)
            return result
        else:
            self.hook.run(sql=query_string,)
            pass

    def _unload_redshift_table(self, ):
        if self.migration:
            self.s3_location = 's3://{s3_bucket}/{s3_prefix}/{dag_run_uuid}'.format(
                s3_bucket=self.s3_bucket,
                s3_prefix=self.s3_prefix,
                dag_run_uuid=self.dag_run_uuid,
            )
        else:
            self.s3_location = 's3://{s3_bucket}/{s3_prefix}/'.format(
                s3_bucket=self.s3_bucket,
                s3_prefix=self.s3_prefix,
            )

        self.param_dict = dict({
            's3_location': self.s3_location,
            'access_key': Variable.get(key='redshift_access_key'),
            'secret_access_key': Variable.get(key='redshift_secret_access_key'),
            'unload_options': '\n'.join(self.unload_option_list),
            'query': self.query,
        })

        UNLOAD_QUERY_STRING = """
            UNLOAD ('{param[query]}')
            TO '{param[s3_location]}'
            ACCESS_KEY_ID '{param[access_key]}'
            SECRET_ACCESS_KEY '{param[secret_access_key]}'
            {param[unload_options]}
        """

        self._run_query(query_string=UNLOAD_QUERY_STRING.format(param=self.param_dict))

    @get_dag_run_uuid
    def execute(self, context):
        self._unload_redshift_table()
        self.log.info('Unloaded redshift table : {0} to s3_location : {1}'.format(self.table_name, self.s3_location))
