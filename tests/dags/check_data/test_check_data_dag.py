import glob
import logging
import pytest
import yaml

from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook
from airflow.models import DAG, DagBag, TaskInstance
from datetime import datetime
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from operators.athena_operator import AthenaOperator

logger = logging.getLogger(__name__)


# This test checks whether the all DAGs were syntactically correct
def test_bag_dag():
    db = DagBag(dag_folder='./dags/check_data.py', include_examples=False)
    config_files = glob.glob('./pipelines/check_data/**/*.yaml', recursive=True)

    assert len(db.dag_ids) == len(config_files)


# Test that the returned operator is Redshift
@pytest.mark.parametrize('setup_dag_factory', ['test_redshift_redshift_simple_le.yaml', ], indirect=True)
def test_redshift_operator(setup_dag_factory, mocker):
    dag = setup_dag_factory.create()

    get_addend_data = dag.get_task(task_id='get_addend_data')
    get_augend_data = dag.get_task(task_id='get_augend_data')

    logger.info(get_addend_data)
    logger.info(get_augend_data)

    assert isinstance(dag.get_task(task_id='get_addend_data'), RedshiftOperator)
    assert isinstance(dag.get_task(task_id='get_augend_data'), RedshiftOperator)


## Test that the results are pushed to xcom correctly in Redshift
@pytest.mark.parametrize('setup_dag_factory', ['test_redshift_redshift_simple_le.yaml', ], indirect=True)
def test_redshift_xcom_push(setup_dag_factory, mocker):
    dag = setup_dag_factory.create()

    mocker.patch.object(
        RedshiftOperator,
        '_run_query',
        side_effect=[
            [{'result': 1}],
            [{'result': 2}],
        ],
    )

    get_addend_data = dag.get_task(task_id='get_addend_data')
    get_augend_data = dag.get_task(task_id='get_augend_data')

    ti_get_addend_data = TaskInstance(task=get_addend_data, execution_date=datetime.now())
    ti_get_augend_data = TaskInstance(task=get_augend_data, execution_date=datetime.now())

    mocker.patch.object(ti_get_addend_data, 'xcom_push')
    mocker.patch.object(ti_get_augend_data, 'xcom_push')

    result_1 = get_addend_data.execute(ti_get_addend_data.get_template_context())
    result_2 = get_augend_data.execute(ti_get_augend_data.get_template_context())

    logger.info(result_1)
    logger.info(result_2)

    ti_get_addend_data.xcom_push.assert_called_once_with(key='query_result', value=[{'result': 1}])
    ti_get_augend_data.xcom_push.assert_called_once_with(key='query_result', value=[{'result': 2}])


## Test that the comparison works correctly
@pytest.mark.parametrize('setup_dag_factory', ['test_comparison_simple_eq.yaml', ], indirect=True)
def test_comparison(setup_dag_factory, mocker):
    dag = setup_dag_factory.create()

    mocker.patch.object(
        TaskInstance,
        'xcom_pull',
        side_effect=[
            [{'result': 1}, ],
            [{'result': 2}, ]
        ]
    )

    compare_data = dag.get_task(task_id='compare_data')
    ti_compare_data = TaskInstance(task=compare_data, execution_date=datetime.now())
    result = compare_data.execute(ti_compare_data.get_template_context())

    # Assert that (1*1) + 2*(-1) < 0  is True
    assert result is True

# Test that the returned operator is Athena
@pytest.mark.parametrize('setup_dag_factory', ['test_athena_athena_simple_le.yaml', ], indirect=True)
def test_athena_operator(setup_dag_factory, mocker):
    dag = setup_dag_factory.create()

    get_addend_data = dag.get_task(task_id='get_addend_data')
    get_augend_data = dag.get_task(task_id='get_augend_data')

    logger.info(get_addend_data)
    logger.info(get_augend_data)

    assert isinstance(dag.get_task(task_id='get_addend_data'), AthenaOperator)
    assert isinstance(dag.get_task(task_id='get_augend_data'), AthenaOperator)


## Test that the results are pushed to xcom correctly in Athena
@pytest.mark.parametrize('setup_dag_factory', ['test_athena_athena_simple_le.yaml', ], indirect=True)
def test_athena_xcom_push(setup_dag_factory, mocker):
    dag = setup_dag_factory.create()

    mocker.patch.object(AthenaOperator, 'execute', side_effect=None) # patch to return none
    mocker.patch.object(
        AWSAthenaHook, 
        'get_query_results', 
        side_effect=[
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "result"}]},
                        {"Data": [{"VarCharValue": "2"}]},
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "Name": "result",
                                "Type": "bigint",
                            },
                        ]
                    }
                },
            },
            {
                "ResultSet": {
                    "Rows": [
                        {"Data": [{"VarCharValue": "result"}]},
                        {"Data": [{"VarCharValue": "1"}]},
                    ],
                    "ResultSetMetadata": {
                        "ColumnInfo": [
                            {
                                "Name": "result",
                                "Type": "bigint",
                            },
                        ]
                    }
                },
            },
        ]
    )

    get_augend_data = dag.get_task(task_id='get_augend_data')
    get_addend_data = dag.get_task(task_id='get_addend_data')

    ti_get_augend_data = TaskInstance(task=get_augend_data, execution_date=datetime.now())
    ti_get_addend_data = TaskInstance(task=get_addend_data, execution_date=datetime.now())

    mocker.patch.object(ti_get_augend_data, 'xcom_push')
    mocker.patch.object(ti_get_addend_data, 'xcom_push')

    get_augend_data.hook = get_augend_data.get_hook()
    get_addend_data.hook = get_addend_data.get_hook()

    get_augend_data.post_execute(ti_get_augend_data.get_template_context(), 'query-execution-id')
    get_addend_data.post_execute(ti_get_addend_data.get_template_context(), 'query-execution-id')
    
    ti_get_augend_data.xcom_push.assert_called_once_with(key='query_result', value=[{'result': 2}])
    ti_get_addend_data.xcom_push.assert_called_once_with(key='query_result', value=[{'result': 1}])
    
