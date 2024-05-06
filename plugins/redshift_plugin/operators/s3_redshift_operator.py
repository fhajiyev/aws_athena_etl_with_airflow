from __future__ import annotations

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, Variable
from plugins.redshift_plugin.hooks.redshift_hook import RedshiftHook
from plugins.redshift_plugin.constants import RedshiftLoadType
from psycopg2 import InternalError as InternalError
from utils.decorators import get_dag_run_uuid


class S3RedshiftOperator(BaseOperator):
    """
    Executes a COPY query operation of data from S3 to Redshift

    :param table_name: name of the target redshift table
    :type: string

    :param columns: column names of target redshift table
    :type: list(string)
    """

    template_fields = ('s3_prefix', )
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        columns: list(str),

        copy_method: RedshiftLoadType,
        copy_option_list: list(str),

        s3_bucket: str,
        s3_prefix: str,
        s3_prefix_dag_run_uuid: bool = True,

        increment_key: str = None,  # Required for : DEDUPLICATED_UPSERT
        unique_key_list: list(str) = None,  # Required for : UPSERT, DEDUPLCATED_UPSERT

        deduplicate_key_list: list(str) = None,  # Required for : DEDUPLCATED_UPSERT

        redshift_conn_id: str = 'redshift',
        jsonpath_location: str = None,
        timeformat: str = None,
        database: str = None,

        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.columns = columns
        self.increment_key = increment_key
        self.unique_key_list = unique_key_list
        self.deduplicate_key_list = deduplicate_key_list or unique_key_list
        self.copy_method = copy_method
        self.copy_option_list = copy_option_list

        self.redshift_conn_id = redshift_conn_id
        self.database = database

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_prefix_dag_run_uuid = s3_prefix_dag_run_uuid
        self.jsonpath_location = jsonpath_location
        self.timeformat = timeformat or 'auto'

        self.dag_run_uuid = None

        self.UPSERT_STRING = """
        BEGIN;
        CREATE TABLE IF NOT EXISTS {param[table_name]}_staging_{param[dag_run_uuid]} (LIKE {param[table_name]});

        COPY {param[table_name]}_staging_{param[dag_run_uuid]}
        ( {param[columns]} )
        FROM '{param[data_source]}'
        ACCESS_KEY_ID '{param[access_key]}'
        SECRET_ACCESS_KEY '{param[secret_access_key]}'
        {param[copy_options]};

        DELETE FROM {param[table_name]}
        USING {param[table_name]}_staging_{param[dag_run_uuid]}
        WHERE {param[unique_condition]};

        {param[insert_subquery]}

        DROP TABLE {param[table_name]}_staging_{param[dag_run_uuid]};
        END;
        """

        self.COPY_STRING = """
        BEGIN;
        {param[delete_string]}
        COPY {param[table_name]}
        ( {param[columns]} )
        FROM '{param[data_source]}'
        ACCESS_KEY_ID '{param[access_key]}'
        SECRET_ACCESS_KEY '{param[secret_access_key]}'
        {param[copy_options]};
        END;
        """

        self.DEDUPLICATED_INSERT_SUBQUERY = """
        INSERT INTO {param[table_name]}
        (
            SELECT
                {param[columns]}
            FROM
                (
                SELECT
                    *, ROW_NUMBER() OVER (PARTITION BY {param[deduplicate_by]} ORDER BY {param[increment_key]} DESC) AS rownum
                FROM
                    {param[table_name]}_staging_{param[dag_run_uuid]}
                )
            WHERE rownum = 1
        );
        """

        self.INSERT_SUBQUERY = """
        INSERT INTO {param[table_name]}
        (
            SELECT
                {param[columns]}
            FROM
                {param[table_name]}_staging_{param[dag_run_uuid]}
        );
        """

        self.DELETE_STRING = """
        DELETE {table_name};
        """

    def _run_copy_query(self, query_string,):
        self.hook = RedshiftHook(redshift_conn_id=self.redshift_conn_id, schema=self.database)
        try:
            self.hook.run(sql=query_string,)
        except InternalError as e:
            self.log.info(str(e))
            if all(x in str(e) for x in ["The specified S3 prefix", "does not exist"]):
                self.log.info("S3 prefix does not exist, skipping this dag run.")
            else:
                raise e
        pass

    def _incremental_load(self, ) -> None:
        incremental_load_query_string = self.COPY_STRING.format(param=self.base_param_dict)
        self._run_copy_query(incremental_load_query_string)

    def _upsert(self, ):
        unique_condition = ' AND '.join(
            '{table_name}.{key} = {table_name}_staging_{dag_run_uuid}.{key}'.format(
                table_name=self.table_name,
                dag_run_uuid=self.dag_run_uuid,
                key=key,
            ) for key in self.unique_key_list
        )

        self.upsert_param_dict = dict({
            'unique_condition': unique_condition,
            'dag_run_uuid': self.dag_run_uuid,
            'insert_subquery': self.insert_string
        })

        upsert_param_dict = {**self.base_param_dict, **self.upsert_param_dict}
        upsert_query_string = self.UPSERT_STRING.format(param=upsert_param_dict)
        self._run_copy_query(upsert_query_string)

    @get_dag_run_uuid
    def execute(self, context):
        self.log.info("Copying data from S3 to Redshift")

        if self.s3_prefix_dag_run_uuid:
            self.s3_prefix = '/'.join([self.s3_prefix, self.dag_run_uuid])

        self.s3_location = 's3://{s3_bucket}/{s3_prefix}'.format(
            s3_bucket=self.s3_bucket,
            s3_prefix=self.s3_prefix,
        )

        self.base_param_dict = dict({
            'table_name': self.table_name,
            'columns': ', '.join(self.columns),
            'data_source': self.s3_location,
            'access_key': Variable.get(key='redshift_access_key'),
            'secret_access_key': Variable.get(key='redshift_secret_access_key'),
            'copy_options': '\n'.join(self.copy_option_list),
        })

        if self.jsonpath_location:
            self.base_param_dict['copy_options'] = self.base_param_dict['copy_options'].format(
                jsonpath_location=self.jsonpath_location,
                timeformat=self.timeformat,
            )

        # REPLACE load type truncates the target table before incremental load
        if self.copy_method == RedshiftLoadType.REPLACE:
            self.base_param_dict['delete_string'] = self.DELETE_STRING.format(table_name=self.table_name)
            self._incremental_load()

        elif self.copy_method == RedshiftLoadType.INCREMENTAL:
            self.base_param_dict['delete_string'] = ''
            self._incremental_load()

        elif self.copy_method == RedshiftLoadType.DEDUPLICATED_UPSERT:
            insert_param_dict = dict({
                'table_name': self.table_name,
                'columns': ', '.join(self.columns),
                'dag_run_uuid': self.dag_run_uuid,
                'increment_key': self.increment_key,
                'deduplicate_by': ', '.join(self.deduplicate_key_list),
            })
            self.insert_string = self.DEDUPLICATED_INSERT_SUBQUERY.format(param=insert_param_dict)
            self._upsert()

        elif self.copy_method == RedshiftLoadType.UPSERT:
            insert_param_dict = dict({
                'table_name': self.table_name,
                'columns': ', '.join(self.columns),
                'dag_run_uuid': self.dag_run_uuid,
            })
            self.insert_string = self.INSERT_SUBQUERY.format(param=insert_param_dict)
            self._upsert()

        else:
            raise NotImplementedError("The copy method {wrong_method} provided is not implemented. Choose from {copy_methods}".format(
                wrong_method=self.copy_method,
                copy_methods=", ".join([e.value for e in RedshiftLoadType])
            ))
