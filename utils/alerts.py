from abc import ABC, abstractmethod
from airflow.utils.log.logging_mixin import LoggingMixin
from dataclasses import dataclass
from functools import partial
from utils.slack import task_sla_miss_slack_alert, task_fail_slack_alert, task_retry_slack_alert, SLACK_CHANNEL_MAP


@dataclass(repr=True, )
class AlertConfig:
    """
    :param destination: The destination of the alert (Slack, Email, etc.)
    :type destination: str

    :param destination_args: Destination specific arguments
    :type destination_args: str

    :param trigger: Trigger event for the alert
    :type trigger: str
    """
    destination: str
    destination_args: dict()
    trigger: str


class AlertBuilder(LoggingMixin):
    """
    AlertBuilder builds an Alert object from trigger and destination data and returns the config
    """
    def __init__(self, alert_config: AlertConfig):
        self._alert_config = alert_config
        self._alert = None

    def build(self) -> dict():
        if self._alert_config.destination == 'slack':
            self._alert = SlackAlert(trigger=self._alert_config.trigger, destination_args=self._alert_config.destination_args)
        elif self._alert_config.destination == 'email':
            raise NotImplementedError
        else:
            raise NotImplementedError

        self._alert._configure()
        return self._alert.get_config()


class Alert(ABC):
    def __init__(self, trigger: str, destination_args: dict()):
        self._trigger = trigger
        self._destination_args = destination_args
        self._config = dict()

    @abstractmethod
    def _configure(self, ):
        raise NotImplementedError

    def get_config(self, ) -> dict():
        return self._config


class SlackAlert(Alert):
    def _configure(self, ):
        AlertTriggerMap = dict({
            'failure': {'on_failure_callback': partial(task_fail_slack_alert, channel=self._destination_args['channel'])},
            'sla_miss': {'sla_miss_callback': partial(task_sla_miss_slack_alert, channel=self._destination_args['channel'])},
            'retry': {'on_retry_callback': partial(task_retry_slack_alert, channel=self._destination_args['channel'])}
        })
        self._config.update(AlertTriggerMap[self._trigger])


class EmailAlert(Alert):
    def _configure(self, ):
        # TODO: Implement
        raise NotImplementedError


class PagerAlert(Alert):
    def _configure(self, ):
        # TODO: Implement
        raise NotImplementedError
