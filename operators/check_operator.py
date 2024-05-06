# -*- coding: utf-8 -*-

import operator

from uuid import uuid4

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CheckOperator(BaseOperator):
    """
    To compare XCOM value with threshold value and return or drop exception if comparison fails

    :param threshold_value: Value to be compared with XCOM variable
    :type threshold_value: int
    :param comparison_op: Comparison operator
    :type comparison_op: string
    :param xcom_task_id: Task from which XCOM values are received
    :type xcom_task_id: string
    """

    @apply_defaults
    def __init__(self,
                 threshold_value,
                 comparison_op,
                 xcom_task_id,
                 *args,
                 **kwargs):
        super(CheckOperator, self).__init__(*args, **kwargs)
        self.threshold_value = threshold_value
        self.comparison_op = comparison_op
        self.xcom_task_id = xcom_task_id

    def execute(self, context):
        pulled_value = int(context['task_instance'].xcom_pull(key='query_result'))

        op = getattr(operator, self.comparison_op, None)
        if op not in {operator.lt, operator.le, operator.eq, operator.ge, operator.gt}:
            raise AttributeError(f"Operator : {op} is not supported. Choose one from 'lt', 'le', 'eq', 'ge', 'gt'")

        if op(pulled_value, self.threshold_value):
            raise AirflowFailException(f"{pulled_value}  {op}  {self.threshold_value} evaluated TRUE")

        return
