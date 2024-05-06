# -*- coding: utf-8 -*-

from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.utils.decorators import apply_defaults


class AthenaOperator(AWSAthenaOperator):
    """
    To enable operator to delete pre-existed data in query result destination.

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :type bucket: str
    :param prefix: The prefix of the s3 object(s). (templated)
    :type prefix: str
    :param push_xcom: push query result to xcom
    :type push_xcom: bool
    """

    template_fields = ('query', 'database', 'output_location', 'bucket', 'prefix')

    @apply_defaults
    def __init__(self,
                 clean_path=False,
                 bucket=None,
                 prefix=None,
                 push_xcom=False,
                 *args,
                 **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)
        self.clean_path = clean_path
        self.bucket = bucket
        self.prefix = prefix
        self.push_xcom = push_xcom

    def _print_query(self, ):
        self.log.info('\n' + self.query) # add newline to readability

    def _clear_s3_prefix(self, ):
        if not self.clean_path:
            return

        if self.bucket is None or self.prefix is None:
            raise AirflowException("Wrong bucket or prefix name.")

        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            'Getting the list of files from bucket: %s in prefix: %s',
            self.bucket, self.prefix
        )

        keys = hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix)

        if keys is None:
            self.log.info(
                'There are no files under the prefix: %s of bucket: %s',
                self.prefix, self.bucket
            )
            return

        response = hook.delete_objects(bucket=self.bucket, keys=keys)

        deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
        self.log.info("Deleted: %s", deleted_keys)

        if "Errors" in response:
            errors_keys = [x['Key'] for x in response.get("Errors", [])]
            raise AirflowException("Errors when deleting: {}".format(errors_keys))
        return

    def pre_execute(self, context):
        self._print_query()
        self._clear_s3_prefix()

    def post_execute(self, context, result=None):
        if self.push_xcom:
            self.__push_query_result_to_xcom(context, result)

    def __push_query_result_to_xcom(self, context, result):
        query_result = self.hook.get_query_results(query_execution_id=result)
        self.log.info(f'raw query {query_result}')

        cols = self.__parse_columns(query_result)

        result = self.__parse_query_result_data(cols, query_result)
        next_token = query_result.get('NextToken')
        while next_token:
            query_result = self.hook.get_query_results(query_execution_id=self.query_execution_id)

            result_slice = self.__parse_query_result_data(cols, query_result)
            result.extend(result_slice)
            next_token = query_result.get('NextToken')

        self.log.info(f'Pushing query results {result}: to XCOM')
        context['ti'].xcom_push(key='query_result', value=result)

    def __parse_columns(self, query_result):
        cols = []
        for c in query_result['ResultSet']['ResultSetMetadata']['ColumnInfo']:
            col = Column(c['Name'], c['Type'])
            cols.append(col)
        return cols

    def __parse_query_result_data(self, cols, query_result):
        if len(query_result['ResultSet']['Rows']) == 1: # no query result
            raise AirflowException('No query result to push xcom')

        parsed = []
        for r in query_result['ResultSet']['Rows'][1:]:
            d = {}
            for i, field in enumerate(r["Data"]):
                col = cols[i]
                print(f'{i}, {field.get("VarCharValue")}')
                d[col.name] = col.transform(field.get('VarCharValue'))
            parsed.append(d)
        return parsed

def transform_bool(value):
    return bool(value or False)

def transform_int(value):
    return int(value or 0)

def transform_float(value):
    return float(value or 0)

def transform_str(value):
    return str(value or '')

class Column():
    # column info: https://docs.aws.amazon.com/ko_kr/athena/latest/APIReference/API_ColumnInfo.html
    # data types: https://docs.aws.amazon.com/ko_kr/athena/latest/ug/data-types.html
    VALUE_TRANSFORMER = {
        'BOOLEAN': transform_bool,

        'TINYINT': transform_int,
        'SMALLINT': transform_int,
        'INT': transform_int,
        'INTEGER': transform_int,
        'BIGINT': transform_int,

        'DOUBLE': transform_float,
        'DECIMAL': transform_float,
        'FLOAT': transform_float,

        'CHAR': transform_str,
        'VARCHAR': transform_str,
        'STRING': transform_str,
        'DEFAULT': transform_str,
    }

    def __init__(self, col_name, col_type):
        self.name = col_name
        self.transformer = self.__get_transformer(col_type.upper())

    def __get_transformer(self, col_type):
        if col_type not in self.VALUE_TRANSFORMER:
            raise AirflowException(f'Implement transformer for column type {col_type}')
        return self.VALUE_TRANSFORMER[col_type]

    def transform(self, value):
        return self.transformer(value)

# get_query_results example
# query: SELECT SUM(reward) as result FROM prod_statssvc.g_unit_ad_reward WHERE data_at >= TIMESTAMP '2020-10-28 10:00:00' AND data_at <  TIMESTAMP '2020-10-28 11:00:00'
# return: {"UpdateCount": 0, "ResultSet": {"Rows": [{"Data": [{"VarCharValue": "result"}]}, {"Data": [{"VarCharValue": "372"}]}], "ResultSetMetadata": {"ColumnInfo": [{"CatalogName": "hive", "SchemaName": "", "TableName": "", "Name": "result", "Label": "result", "Type": "bigint", "Precision": 19, "Scale": 0, "Nullable": "UNKNOWN", "CaseSensitive": false}]}}, "ResponseMetadata": {"RequestId": "db513763-b2ba-4f45-9b24-6cc5ffb627e2", "HTTPStatusCode": 200, "HTTPHeaders": {"content-type": "application/x-amz-json-1.1", "date": "Sat, 14 Nov 2020 07:56:01 GMT", "x-amzn-requestid": "db513763-b2ba-4f45-9b24-6cc5ffb627e2", "content-length": "558", "connection": "keep-alive"}, "RetryAttempts": 0}}
