import os
import pytest

from tests.dag_builders import TestSimpleDagBuilder
from utils.dag import config_builder, DagFactory


PIPELINES_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'files',
)

DEFAULT_ARGS = {
    'owner': 'test',
    'depends_on_past': True,
    'backfill': True,
    'concurrency': 1,
}

@pytest.fixture
def setup_dag_factory(request):
    return DagFactory(dag_builder=TestSimpleDagBuilder, dag_config=config_builder(default_args=DEFAULT_ARGS, file=os.path.join(PIPELINES_DIR, request.param), pipeline_type='test'))


@pytest.helpers.register
def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"]
    )
