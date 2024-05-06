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

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults


class S3RenameObjectsOperator(BaseOperator):
    ui_color = '#44b5e2'
    template_fields = ('bucket', 'prefix', 'file_key', 's3_conn_id')

    @apply_defaults
    def __init__(self, bucket, prefix, file_key, file_extension, s3_conn_id='adfit_s3', *args, **kwargs):
        super(S3RenameObjectsOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.file_key = file_key
        self.file_extension = file_extension
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        s3_conn = S3Hook(aws_conn_id=self.s3_conn_id)

        bucket = s3_conn.get_bucket(self.bucket)
        objects = bucket.objects.filter(Prefix=self.prefix)

        for i, object in enumerate(objects):
            obj_name = '{file_key}_{id}.{file_extension}'.format(
                file_key=self.file_key,
                id=str(i).zfill(3),
                file_extension=self.file_extension
            )
            dest_object_key = '/'.join([self.prefix, obj_name])

            self.log.info("Relocating query result from {src} to {dest}".format(
                src=object.key,
                dest=dest_object_key,
            ))

            s3_conn.copy_object(
                source_bucket_key=object.key,
                dest_bucket_key=dest_object_key,
                source_bucket_name=self.bucket,
                dest_bucket_name=self.bucket,
            )

            s3_conn.delete_objects(
                bucket=self.bucket,
                keys=object.key,
            )
