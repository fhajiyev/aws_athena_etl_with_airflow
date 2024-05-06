from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.dag import DagBuilder


class TestSimpleDagBuilder(DagBuilder, LoggingMixin):
    def build_dag(self):
        dag = DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
        )
        with dag:
            DummyOperator(task_id='task_1')
        return dag