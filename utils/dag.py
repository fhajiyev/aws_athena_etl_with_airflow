import copy
import datetime
import logging
import yaml

from abc import ABC, abstractmethod
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor, ExternalTaskMarker
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from utils.alerts import AlertBuilder, AlertConfig
from dataclasses import dataclass
from datetime import timedelta
from typing import List


logger = logging.getLogger(__name__)


@dataclass(repr=True, )
class ExternalUpstreamDepencency:
    """
    :param dag_id:
    :type dag_id: str

    :param execution_delta:
    :type execution_delta: datetime.timedela
    """
    dag_id: str
    execution_delta: timedelta


@dataclass(repr=True, )
class ExternalDownstreamDepencency:
    """
    :param dag_id:
    :type dag_id: str

    :param task_id:
    :type task_id: str
    """
    dag_id: str
    task_id: str


@dataclass(repr=True, )
class DagConfig:
    """
    :param pipeline_config:
    :type pipeline_config: dict()

    :param schedule_interval: datetime.timedelta or
        dateutil.relativedelta.relativedelta or str that acts as a cron
        expression

    :param default_args:
    :type default_args:

    :param dag_id:
    :type dag_id: str

    :param upstream_dependencies: List of dag ids that is upstream of this dag
    :type upstream_dependencies: list(ExternalUpstreamDepencency)

    :param downstream_dependencies: List of dag ids that is downstream of this dag
    :type downstream_dependencies: list(ExternalDownstreamDepencency)

    :param alert_configs: list(AlertConfig): List of alert configurations
    :type alert_configs: list(AlertConfig)

    :param execution_delay: In seconds, for how long the operational part of the DagRun should wait after the execution_date
    :type execution_delay: int
    """
    pipeline_config: dict()
    schedule_interval: str
    default_args: dict()
    dag_id: str
    upstream_dependencies: List[ExternalUpstreamDepencency] = None
    downstream_dependencies: List[ExternalDownstreamDepencency] = None
    alert_configs: List[AlertConfig] = None
    execution_delay: int = None


class DagBuilder(ABC):
    def __init__(self, dag_config: DagConfig):
        self.dag_id: str = dag_config.dag_id
        self.default_args: dict() = dag_config.default_args
        self.pipeline_config: dict() = dag_config.pipeline_config
        self.schedule_interval: str = dag_config.schedule_interval

    @abstractmethod
    def build_dag(self, ) -> DAG:
        """Builds the dag with the specified dag parameters"""
        raise NotImplementedError


class DagFactory(LoggingMixin):
    """
    DagFactory is initialized with a DagConfig and a DagBuilder.
    It
    1. builds a DAG object with the building process provided by the DagBuilder class,
    2. accessorizes it with alerts, external_dependencies and etc,
    3. registers the built dag to the DagBag
    """
    def __init__(self, dag_config: DagConfig, dag_builder: DagBuilder):
        self._dag_config = dag_config
        self._configure_alert()
        self._dag_builder = dag_builder(dag_config=self._dag_config)
        self._dag = None

    def _build_dag(self, ):
        self.log.info('Building DAG')
        self._dag = self._dag_builder.build_dag()

    def _configure_alert(self, ):
        if self._dag_config.alert_configs:
            self._set_alerts()

    def _accessorize_dag(self, ):
        if self._dag_config.upstream_dependencies:
            self._set_upstream_dependency()
        if self._dag_config.downstream_dependencies:
            self._set_downstream_dependency()
        if self._dag_config.execution_delay:
            self._set_execution_delay()


    def _set_downstream_dependency(self, ):
        leaves = self._dag.leaves

        for dd in self._dag_config.downstream_dependencies:
            self.log.info(f'Adding downstream dependency : {dd.dag_id}_{dd.task_id}')

            mark_downstream = ExternalTaskMarker(
                task_id=f'check_{dd.dag_id}_{dd.task_id}',
                external_dag_id=dd.dag_id,
                external_task_id=dd.task_id,
                dag=self._dag
            )

            leaves >> mark_downstream

    def _set_upstream_dependency(self, ):
        roots = self._dag.roots

        for ud in self._dag_config.upstream_dependencies:
            self.log.info(f'Adding upstream dependency : {ud.dag_id}')

            sense_upstream = ExternalTaskSensor(
                task_id=f'check_{ud.dag_id}',
                external_dag_id=ud.dag_id,
                execution_delta=ud.execution_delta,
                dag=self._dag,
                timeout=10,
                poke_interval=10,
                retry_delay=datetime.timedelta(minutes=1),
                retry_exponential_backoff=True,
                retries=8,
                check_existence=True,
                on_retry_callback=None,
            )

            sense_upstream >> roots

    def _set_execution_delay(self, ):
        roots = self._dag.roots
        self.log.info(f'Adding execution delay of {self._dag_config.execution_delay}')

        retry_delay = self._dag_config.execution_delay
        retries = 2
        # retry delay가 1시간을 넘을경우 1시간 단위로 retry하도록 함
        if self._dag_config.execution_delay > 3600:
            retry_delay = 3600
            retries = (self._dag_config.execution_delay // 3600) + 2 # delay가 1시간 30분일경우, 최초, 1시간뒤, 2시간뒤

        delay_execution = TimeDeltaSensor(
            task_id='delay_execution',
            timeout=10,
            poke_interval=10,
            delta=timedelta(seconds=self._dag_config.execution_delay),
            retry_delay=timedelta(seconds=retry_delay),
            retries=retries,
            dag=self._dag,
            on_retry_callback=None,
        )

        delay_execution >> roots

    def _set_alerts(self, ):
        for alert_config in self._dag_config.alert_configs:
            self.log.info(f'Adding alerts with config {alert_config}')
            alert_builder = AlertBuilder(alert_config=alert_config)
            self._dag_config.default_args.update(alert_builder.build())

    def get_dag_id(self, ):
        return self._dag_config.dag_id

    def create(self, ):
        self.log.info(f'Creating DAG: {self._dag_config.dag_id}')
        self._build_dag()
        self._accessorize_dag()
        return self._dag


def config_builder(default_args, file, pipeline_type):
    """
    Scans through the given directory and generates pipeline_configs that corresponds to a given pipeline_type
    """

    upstream_dependencies = []
    downstream_dependencies = []
    alert_configs = []
    execution_delay = None
    schedule_interval = None
    default_args = copy.copy(default_args)

    with open(file, 'r') as pipeline_config:
        logger.info(pipeline_config)
        pipeline_config = yaml.safe_load(pipeline_config)
        if pipeline_config['pipeline_type'] == pipeline_type:

            dag_id = f"{pipeline_type}_{pipeline_config['pipeline_key']}"

            if 'pipeline_dag_configs' in pipeline_config.keys():
                schedule_interval = pipeline_config.get('pipeline_dag_configs', {}).pop('schedule_interval', None)

                logger.info("Updated dag arguments with {}".format(dict(pipeline_config['pipeline_dag_configs'])))
                default_args.update(dict(pipeline_config['pipeline_dag_configs']))

            if 'upstream_dependencies' in pipeline_config.keys():
                upstream_dependencies = [ExternalUpstreamDepencency(dag_id=conf['dag_id'], execution_delta=timedelta(days=conf.get('timedelta_day', 0), hours=conf.get('timedelta_hours', 0))) for conf in pipeline_config['upstream_dependencies']]

            if 'downstream_dependencies' in pipeline_config.keys():
                downstream_dependencies = [ExternalDownstreamDepencency(dag_id=conf['dag_id'], task_id=conf['task_id']) for conf in pipeline_config['downstream_dependencies']]

            if 'alerts' in pipeline_config.keys():
                alert_configs = [AlertConfig(destination=dest, destination_args=config['args'], trigger=config['trigger']) for dest, configs in pipeline_config['alerts'].items() for config in configs]

            if 'execution_delay' in pipeline_config.keys():
                execution_delay = pipeline_config['execution_delay']

            logger.info(f'Processed yaml configuration for DAG: {dag_id}')

            return DagConfig(default_args=default_args, dag_id=dag_id, schedule_interval=schedule_interval, pipeline_config=pipeline_config, upstream_dependencies=upstream_dependencies, downstream_dependencies=downstream_dependencies, alert_configs=alert_configs, execution_delay=execution_delay)
