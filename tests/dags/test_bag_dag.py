import glob
import logging
import pytest

from airflow.models import DagBag

logger = logging.getLogger(__name__)


# This test checks whether the all DAGs were syntactically correct
@pytest.mark.parametrize('dag_template_key', ['athena_process', 'athena_catalog', 'redshift_query', 'redshift_transform_load', 's3_redshift_sync', 'mysql_unload_s3'])
def test_bag_dag(dag_template_key):
    logger.info(f'Key : {dag_template_key}')
    db = DagBag(dag_folder=f'./dags/{dag_template_key}.py', include_examples=False)
    config_files = glob.glob(f'./pipelines/{dag_template_key}/**/*.yaml')
    logger.info(f'Asserting {len(db.dag_ids)} == {len(config_files)}')
    assert len(db.dag_ids) == len(config_files)
    return
