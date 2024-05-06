# -*- coding: utf-8 -*-

from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.utils.decorators import apply_defaults


class AthenaGetResultsOperator(AWSAthenaOperator):
    """
    To run Athena query and send its results to XCOM
    This operator assumes a single row/single column query result having the following format:

    {
        'ResultSet': {
            'Rows':[
                {
                    'Data':[
                        {
                            'VarCharValue':<result alias>
                        }
                    ]
                },
                {
                    'Data':[
                        {
                            'VarCharValue':<actual result>
                        }
                    ]
                }
            ]
        }
    }

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :type bucket: str
    :param prefix: The prefix of the s3 object(s). (templated)
    :type prefix: str
    :param clean_path: Indicates whether a given bucket/prefix should be cleared prior to running query
    :type clean_path: bool
    """

    template_fields = ('query', 'database', 'output_location', 'bucket', 'prefix')

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix,
                 clean_path=False,
                 *args,
                 **kwargs):
        super(AthenaGetResultsOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.clean_path = clean_path

    def _print_query(self, ):
        self.log.info(self.query)

    def _clear_s3_prefix(self, bucket, prefix):
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            'Getting the list of files from bucket: %s in prefix: %s',
            bucket, prefix
        )

        keys = hook.list_keys(
            bucket_name=bucket,
            prefix=prefix)

        if keys is None:
            self.log.info(
                'There are no files under the prefix: %s of bucket: %s',
                prefix, bucket
            )
            return

        response = hook.delete_objects(bucket=bucket, keys=keys)

        deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
        self.log.info("Deleted: %s", deleted_keys)

        if "Errors" in response:
            error_tuples = [(x['Key'], x['Code']) for x in response.get("Errors", [])]
            raise AirflowFailException(f"Errors when deleting: {error_tuples[0]}. The error code is : {error_tuples[1]}")
        return

    def pre_execute(self, context):
        self._print_query()
        self._clear_s3_prefix(self.bucket, self.prefix)

    def post_execute(self, context, result=None):
        """
        This hook is triggered right after self.execute() is called.
        It is passed the execution context and any results returned by the
        operator.
        """
        hook = self.get_hook()
        query_result = hook.get_query_results(self.query_execution_id)
        context['task_instance'].xcom_push(key='query_result', value=query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
