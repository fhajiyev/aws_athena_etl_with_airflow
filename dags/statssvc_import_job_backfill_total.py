from functools import partial

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from utils.slack import task_fail_slack_alert, task_retry_slack_alert

statssvc_alert_channel_id = 'alert-nathan'
env = "{{var.value.get('server_env', 'dev')}}"


class CustomKubernetesPodOperator(KubernetesPodOperator):
    """
    Custom wrapper class to extend the template fields
    """
    template_fields = ('image', 'configmaps', 'cmds')


default_args = {
    'owner': 'statssvc',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 1),
    'end_date': datetime(2020, 11, 23, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(task_fail_slack_alert, channel=statssvc_alert_channel_id),
    'on_retry_callback': partial(task_retry_slack_alert, channel=statssvc_alert_channel_id),
    'task_concurrency': 1,
}

resources = {
    'request_memory': '2Gi',
    'limit_memory': '2Gi',
    'request_ephemeral_storage': '100Mi',
    'limit_ephemeral_storage': '100Mi',
}

dag = DAG(
    'statssvc_import_job_backfill_total',
    default_args=default_args,
    description='temp dag for backfill recent lineitem, creative total of statssvc',
    schedule_interval='@hourly',
)

statssvc_default_kube_kwargs = dict(
    image='591756927972.dkr.ecr.ap-northeast-1.amazonaws.com/statssvc:{env}'.format(env=env),
    image_pull_policy='Always',  # Always pull image to reflect the changes in env tag
    namespace='airflow',  # We should run in service namespace after airflow migration to service cluster
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True,
    configmaps=['statssvc-{env}-config'.format(env=env)],
    resources=resources,
    execution_timeout=timedelta(minutes=20),
)
# Delay all jobs 4 hours from execution time - that's when all data are available for import.
start_date = "{{execution_date}}"
end_date = "{{next_execution_date - macros.timedelta(seconds=1)}}"
start_date_6hrs_delay = "{{execution_date - macros.timedelta(hours=6)}}"
end_date_6hrs_delay = "{{next_execution_date - macros.timedelta(hours=6, seconds=1)}}"
delete_before_create = "--delete_before_create=1"  # Turn this flag to override existing data

lineitem_total_task = CustomKubernetesPodOperator(
    task_id="lineitem_total",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "lineitem_total",
        start_date,
        end_date,
        delete_before_create,
    ],
    name="import-lineitem-total",
    **statssvc_default_kube_kwargs
)

creative_total_task = CustomKubernetesPodOperator(
    task_id="creative_total",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "creative_total",
        start_date,
        end_date,
        delete_before_create,
    ],
    name="import-creative-total",
    **statssvc_default_kube_kwargs
)