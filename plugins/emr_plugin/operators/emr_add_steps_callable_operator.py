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
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook


class EmrAddStepsCallableOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :type job_flow_id: str
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :type job_flow_name: str
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :type cluster_states: list
    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param steps: boto3 style steps to be added to the jobflow. (templated)
    :type steps: list
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    :type do_xcom_push: bool
    """
    template_fields = ['job_flow_id', 'job_flow_name', 'cluster_states', 'steps_callable', 'callable_kwargs']
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            job_flow_id=None,
            job_flow_name=None,
            cluster_states=None,
            aws_conn_id='aws_default',
            steps_callable=None,
            callable_kwargs=None,
            *args, **kwargs):
        if not ((job_flow_id is None) ^ (job_flow_name is None)):
            raise AirflowException('Exactly one of job_flow_id or job_flow_name must be specified.')
        super(EmrAddStepsCallableOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.steps_callable = steps_callable
        self.callable_kwargs = callable_kwargs

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        job_flow_id = self.job_flow_id

        if not job_flow_id:
            job_flow_id = emr.get_cluster_id_by_name(self.job_flow_name, self.cluster_states)

        if self.do_xcom_push:
            context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)

        self.log.info('Adding steps to %s', job_flow_id)
        self.callable_kwargs['context'] = context
        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=self.steps_callable(**self.callable_kwargs))

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('Adding steps failed: %s' % response)
        else:
            self.log.info('Steps %s added to JobFlow', response['StepIds'])
            return response['StepIds']
