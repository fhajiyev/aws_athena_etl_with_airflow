# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from uuid import uuid4

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults
from utils.decorators import get_dag_run_uuid
from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook


class AthenaOperator(BaseOperator):
    """
    An operator that submit presto query to athena.
    :param query: Presto to be run on athena. (templated)
    :type query: str
    :param database: Database to select. (templated)
    :type database: str
    :param output_bucket: s3 bucket to write the query results into.
    :type output_bucket: str
    :param output_prefix: s3 prefix to write the query results into.
    :type output_prefix: str
    :param aws_conn_id: aws connection to use
    :type aws_conn_id: str
    :param sleep_time: Time to wait between two consecutive call to check query status on athena
    :type sleep_time: int
    :param keep_metadata: Whether to keep query_result.csv.metadata with query results
    """

    ui_color = '#44b5e2'
    template_fields = ('query', 'database', 'output_location', 'output_prefix')

    @apply_defaults
    def __init__(self, query, database, output_bucket, output_prefix, file_key=None, aws_conn_id='aws_default', s3_conn_id='adfit_s3', client_request_token=None,
                 query_execution_context=None, result_configuration=None, sleep_time=30, discard_metadata=True, work_group='primary',
                 *args, **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.database = database
        self.output_bucket = output_bucket
        self.output_prefix = output_prefix
        self.output_location = 's3://' + output_bucket + '/' + output_prefix
        self.file_key = file_key
        self.aws_conn_id = aws_conn_id
        self.s3_conn_id = s3_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        self.sleep_time = sleep_time
        self.discard_metadata = discard_metadata
        self.work_group = work_group
        self.query_execution_id = None
        self.hook = None

        self.dag_run_uuid = None

    def get_hook(self):
        return AWSAthenaHook(self.aws_conn_id, self.sleep_time)

    @get_dag_run_uuid
    def pre_execute(self, context):
        """
        Remove any s3 objects that is in the destination prefix to ensure task idempotency
        """
        self.s3_conn = S3Hook(aws_conn_id=self.s3_conn_id)
        response = self.s3_conn.delete_objects(
            bucket=self.output_bucket,
            keys='/'.join([self.output_prefix, str(self.dag_run_uuid)]),
        )
        if response.get('Deleted'):
            self.log.info("Cleaned up destination prefix : {dest}, with {num_objs} objects deleted".format(
                dest='/'.join([self.output_bucket, self.output_prefix, str(self.dag_run_uuid)]),
                num_objs=len(response['Deleted'])
            ))
        elif response.get('Errors'):
            self.log.info(response.get('Errors'))
        else:
            self.log.info("Nothing to clean up in prefix : {dest}".format(
                dest='/'.join([self.output_bucket, self.output_prefix, str(self.dag_run_uuid)]),
            ))

    @get_dag_run_uuid
    def execute(self, context):
        """
        Run Presto Query on Athena
        """
        self.hook = self.get_hook()
        self.hook.get_conn()

        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = '/'.join([self.output_location, str(self.dag_run_uuid)])
        self.log.info(self.query)
        self.query_execution_id = self.hook.run_query(self.query, self.query_execution_context,
                                                      self.result_configuration, self.client_request_token, self.work_group)
        self.hook.poll_query_status(self.query_execution_id)

        if self.file_key:
            self._rename_result()

        task_instance = context['task_instance']
        task_instance.xcom_push('query_execution_id', dict({'query_execution_id': self.query_execution_id}))

    def on_kill(self):
        """
        Cancel the submitted athena query
        """
        if self.query_execution_id:
            self.log.info('⚰️⚰️⚰️ Received a kill Signal. Time to Die')
            self.log.info('Stopping Query with executionId - {queryId}'.format(
                queryId=self.query_execution_id))
            response = self.hook.stop_query(self.query_execution_id)
            http_status_code = None
            try:
                http_status_code = response['ResponseMetadata']['HTTPStatusCode']
            except Exception as ex:
                self.log.error('Exception while cancelling query', ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error('Unable to request query cancel on athena. Exiting')
                else:
                    self.log.info('Polling Athena for query with id {queryId} to reach final state'.format(
                        queryId=self.query_execution_id))
                    self.hook.poll_query_status(self.query_execution_id)

    def _rename_result(self):
        self.s3_conn = S3Hook(aws_conn_id=self.s3_conn_id)

        self.log.info("Relocating query result from {src} to {dest}".format(
            src='/'.join([self.output_bucket, self.output_prefix, str(self.dag_run_uuid), self.query_execution_id + '.csv']),
            dest='/'.join([self.output_bucket, self.output_prefix, str(self.dag_run_uuid), self.file_key + '.csv']),
        ))

        self.s3_conn.copy_object(
            source_bucket_key='/'.join([self.output_prefix, str(self.dag_run_uuid), self.query_execution_id + '.csv']),
            dest_bucket_key='/'.join([self.output_prefix, str(self.dag_run_uuid), self.file_key + '.csv']),
            source_bucket_name=self.output_bucket,
            dest_bucket_name=self.output_bucket,
        )

        self.s3_conn.delete_objects(
            bucket=self.output_bucket,
            keys='/'.join([self.output_prefix, str(self.dag_run_uuid), self.query_execution_id + '.csv']),
        )

        if self.discard_metadata:
            self.s3_conn.delete_objects(
                bucket=self.output_bucket,
                keys='/'.join([self.output_prefix, str(self.dag_run_uuid), self.query_execution_id + '.csv.metadata'])
            )
            self.log.info("Discarded query result metadata")