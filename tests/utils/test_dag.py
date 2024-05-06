import logging
import pytest

from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor, ExternalTaskMarker
from airflow.sensors.time_delta_sensor import TimeDeltaSensor


logger = logging.getLogger(__name__)


@pytest.mark.parametrize('setup_dag_factory', ['test_dag_config.yaml', ], indirect=True)
def test_accessorize_dag(setup_dag_factory, mocker):
    mocker.patch.object(setup_dag_factory, '_set_upstream_dependency')
    mocker.patch.object(setup_dag_factory, '_set_downstream_dependency')

    setup_dag_factory._build_dag()
    setup_dag_factory._accessorize_dag()
    setup_dag_factory._set_upstream_dependency.assert_called_once_with()
    setup_dag_factory._set_downstream_dependency.assert_called_once_with()

    return

@pytest.mark.parametrize('setup_dag_factory', ['test_dag_config.yaml', ], indirect=True)
def test_set_downstream_dependency(setup_dag_factory, mocker):
    """
    Test whether the dependencies are set correctly
    """
    setup_dag_factory._build_dag()
    setup_dag_factory._set_downstream_dependency()
    after_dag = setup_dag_factory._dag

    # The check_downstream tasks should be the leaves now
    for leaf in after_dag.leaves:
        assert isinstance(leaf, ExternalTaskMarker)

    return

@pytest.mark.parametrize('setup_dag_factory', ['test_dag_config.yaml', ], indirect=True)
def test_set_upstream_dependency(setup_dag_factory, mocker):
    """
    Test whether the dependencies are set correctly
    """
    setup_dag_factory._build_dag()
    setup_dag_factory._set_upstream_dependency()
    after_dag = setup_dag_factory._dag

    # The check_downstream tasks should be the roots now
    for root in after_dag.roots:
        assert isinstance(root, ExternalTaskSensor)

    return

@pytest.mark.parametrize('setup_dag_factory', ['test_slack_alerts.yaml', ], indirect=True)
def test_set_alerts(setup_dag_factory, mocker):
    """
    Test whether _set_alerts are called properly
    """
    mocker.patch.object(setup_dag_factory, '_set_alerts')
    setup_dag_factory._configure_alert()
    setup_dag_factory._set_alerts.assert_called_once_with()

    return

@pytest.mark.parametrize('setup_dag_factory', ['test_dag_config.yaml', ], indirect=True)
def test_set_execution_delay(setup_dag_factory, mocker):
    """
    Test whether execution_delay is configured properly
    """
    dag: DAG = setup_dag_factory.create()

    for root in dag.roots:
        assert isinstance(root, TimeDeltaSensor)

    assert len(dag.roots) == 1

@pytest.mark.parametrize('setup_dag_factory', ['test_dag_config.yaml', 'test_slack_alerts.yaml'], indirect=True)
def test_create(setup_dag_factory, mocker):
    mocker.patch.object(setup_dag_factory, '_build_dag')
    mocker.patch.object(setup_dag_factory, '_accessorize_dag')

    setup_dag_factory.create()

    setup_dag_factory._build_dag.assert_called_once_with()
    setup_dag_factory._accessorize_dag.assert_called_once_with()
    return
