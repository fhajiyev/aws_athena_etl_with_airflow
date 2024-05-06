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

from typing import Iterable

from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ClearPrefixOperator(BaseOperator):
    """
    To enable users to delete all objects in specific bucket.

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :type bucket: str
    :param prefix: The prefix of the s3 object(s). (templated)
    :type prefix: str or list
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str

    """
    template_fields = ('bucket', 'prefix')  # type: Iterable[str]
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(S3ClearPrefixOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
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
                'There no files under the prefix: %s of bucket: %s',
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
