# -*- coding: utf-8 -*-

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from plugins.redshift_plugin.hooks.redshift_hook import RedshiftHook


class RedshiftOperator(BaseOperator):
    """
    Executes sql code in a specific Redshift database

    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: string
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql', 'param_dict')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            redshift_conn_id='redshift_conn_id',
            autocommit=False,
            fetch=False,
            param_dict=dict(),
            push_xcom=False,
            database=None,
            xcom_params=None,
            xcom_param_dict=dict(),
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit
        self.fetch = fetch
        self.param_dict = param_dict
        self.push_xcom = push_xcom
        self.database = database
        self.xcom_params = xcom_params
        self.xcom_param_dict = xcom_param_dict

    def _run_query(self, ):
        if bool(self.param_dict) is True:
            self.log.info(self.param_dict)
            self.sql = self.sql.format(param=self.param_dict)
        if bool(self.xcom_param_dict) is True:
            self.log.info(self.xcom_param_dict)
            self.sql = self.sql.format(param=self.xcom_param_dict)

        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id, schema=self.database)

        if self.fetch is True:
            result = self.hook.dictfetchall(sql=self.sql)
            self.log.info(result)
            return result
        else:
            self.hook.run(sql=self.sql)
            pass

    def execute(self, context):
        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id,
                                 schema=self.database)

        if self.xcom_params:
            task_instance = context['task_instance']

            for xcom_query_param in self.xcom_params:
                param = dict(task_instance.xcom_pull(xcom_query_param.xcom_source_task_id, key=xcom_query_param.xcom_param_key))
                self.log.info('Current params {0}'.format(self.param_dict))
                self.log.info('Pulling params {0}'.format(param))
                self.xcom_param_dict.update(param)

        result = self._run_query()

        if self.push_xcom:
            if len(result) == 0:
                raise AirflowException('No query result to push xcom')

            self.log.info(f'Pushing query results {result}: to XCOM')
            context['ti'].xcom_push(key='query_result', value=result)

        return result
