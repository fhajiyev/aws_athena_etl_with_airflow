import os
import pytest

from dags.check_data import CheckDataDagBuilder
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
    return DagFactory(dag_builder=CheckDataDagBuilder, dag_config=config_builder(default_args=DEFAULT_ARGS, file=os.path.join(PIPELINES_DIR, request.param), pipeline_type='check_data'))
