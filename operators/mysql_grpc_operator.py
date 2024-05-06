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

# from plugins.grpc_plugin.hooks.grpc_hook import GrpcHook
from airflow.contrib.hooks.grpc_hook import GrpcHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook
from grpc._channel import _InactiveRpcError

from utils import grpc_mappers, grpc_stubs


class MysqlToGrpcOperator(BaseOperator):
    """
    Calls a gRPC endpoint to execute an action
    :param stub_class: The stub client to use for this gRPC call
    :type stub_class: gRPC stub class generated from proto file
    :param call_func: The client function name to call the gRPC endpoint
    :type call_func: gRPC client function name for the endpoint generated from proto file, str
    :param grpc_conn_id: The connection to run the operators against
    :type grpc_conn_id: str
    :param data: The data to pass to the rpc call
    :type data: A dict with key value pairs as kwargs of the call_func
    :param interceptors: A list of gRPC interceptor objects to be used on the channel
    :type interceptors: A list of gRPC interceptor objects, has to be initialized
    :param custom_connection_func: The customized connection function to return channel object
    :type custom_connection_func: A python function that returns channel object, take in
        a connection object, can be a partial function
    :param streaming: A flag to indicate if the call is a streaming call
    :type streaming: boolean
    :param response_callback: The callback function to process the response from gRPC call
    :type response_callback: A python function that process the response from gRPC call,
        takes in response object and context object, context object can be used to perform
        push xcom or other after task actions
    :param log_response: A flag to indicate if we need to log the response
    :type log_response: boolean
    """

    template_fields = ('sql', 'mapper_func', 'stub_class_func', 'call_func')

    @apply_defaults
    def __init__(self,
                 sql,
                 mapper_func,
                 stub_class_func,
                 call_func,
                 error_list=[],
                 mysql_conn_id='mysql_default',
                 grpc_conn_id="grpc_default",
                 interceptors=None,
                 custom_connection_func=None,
                 streaming=False,
                 response_callback=None,
                 log_response=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.mysql_conn_id = mysql_conn_id
        self.mapper_func = mapper_func
        self.stub_class_func = stub_class_func
        self.call_func = call_func
        self.error_list = error_list
        self.grpc_conn_id = grpc_conn_id
        self.interceptors = interceptors
        self.custom_connection_func = custom_connection_func
        self.streaming = streaming
        self.log_response = log_response
        self.response_callback = response_callback

    def _get_mysql_hook(self):
        return MySqlHook(self.mysql_conn_id)

    def _get_grpc_hook(self):
        return GrpcHook(
            self.grpc_conn_id,
            interceptors=self.interceptors,
            custom_connection_func=self.custom_connection_func
        )

    def execute(self, context):
        mysql_hook = self._get_mysql_hook()
        self.log.info("Connecting Mysql DB")

        self.log.info('Executing: %s', self.sql)
        rows = mysql_hook.get_records(self.sql)

        grpc_hook = self._get_grpc_hook()
        self.log.info("Calling gRPC service")

        stub_class = getattr(grpc_stubs, self.stub_class_func)()
        mapper_func = getattr(grpc_mappers, self.mapper_func)
        requests = mapper_func(rows)

        for request in requests:
            data = {'request': request}
            responses = grpc_hook.run(stub_class, self.call_func, streaming=self.streaming, data=data)
            self.log.info("Send request to gRPC service")

            try:
                for response in responses:
                    self._handle_response(response, context)
            except _InactiveRpcError as e:
                if e.details() in self.error_list:
                    None

    def _handle_response(self, response, context):
        if self.log_response:
            self.log.info(repr(response))
        if self.response_callback:
            self.response_callback(response, context)
