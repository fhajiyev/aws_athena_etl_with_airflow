import logging
import pytest

from utils.slack import task_sla_miss_slack_alert, task_fail_slack_alert, task_retry_slack_alert, SLACK_CHANNEL_MAP


logger = logging.getLogger(__name__)

@pytest.mark.parametrize('setup_dag_factory', ['test_slack_alerts.yaml', ], indirect=True)
def test_slack_alert(setup_dag_factory, mocker):
    """
    Tests whether the callbacks will be forwarding the slack message to the given channel
    """
    test_dag = setup_dag_factory.create()

    assert test_dag.default_args['on_failure_callback'].func == task_fail_slack_alert
    assert test_dag.default_args['on_failure_callback'].keywords['channel'] == 'data-emergency'

    assert test_dag.default_args['on_retry_callback'].func == task_retry_slack_alert
    assert test_dag.default_args['on_retry_callback'].keywords['channel'] == 'data-emergency'

    assert test_dag.default_args['sla_miss_callback'].func == task_sla_miss_slack_alert
    assert test_dag.default_args['sla_miss_callback'].keywords['channel'] == 'data-emergency'

    return
