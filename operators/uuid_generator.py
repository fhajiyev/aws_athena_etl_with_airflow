import uuid

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UUIDGenerator(BaseOperator):
    """
    Generates an UUID and stores in xcom for the duration of the DAG
    """

    @apply_defaults
    def __init__(
        self,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.uuid = uuid.uuid4().hex

    def execute(self, context):
        self.log.info('Generated a new uuid for the DAG RUN: {0}'.format(self.uuid))
        task_instance = context['task_instance']
        task_instance.xcom_push('dag_run_uuid', dict({'dag_run_uuid': self.uuid}))
