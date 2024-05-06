from functools import partial

from airflow import DAG
from datetime import timedelta, datetime
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from utils.slack import task_fail_slack_alert, task_retry_slack_alert

statssvc_alert_channel_id = 'dev-emergency-statssvc'
env = "{{var.value.get('server_env', 'dev')}}"


class CustomKubernetesPodOperator(KubernetesPodOperator):
    """
    Custom wrapper class to extend the template fields
    """
    template_fields = ('image', 'configmaps', 'cmds')


default_args = {
    'owner': 'statssvc',
    'depends_on_past': True,
    'start_date': datetime(2020, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'on_failure_callback': partial(task_fail_slack_alert, channel=statssvc_alert_channel_id),
    'on_retry_callback': partial(task_retry_slack_alert, channel=statssvc_alert_channel_id),
    'task_concurrency': 1,
}

resources = {
    'request_memory': '2Gi',
    'limit_memory': '3Gi',
    'request_ephemeral_storage': '100Mi',
    'limit_ephemeral_storage': '100Mi',
}

dag = DAG(
    'statssvc_update_adn',
    default_args=default_args,
    description='A job that updates adnetwork report and unit finance in statssvc then updates report in billingsvc',
    # NOTE: adnreportsvc crawls at 09 (00 UTC) & 21 (12 UTC) and takes approx. 2hrs, so run this dag after
    schedule_interval='30 3,15 * * *',  # Runs daily at 03:30 & 15:30
    catchup=False,  # Avoid running update job for past days
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
    execution_timeout=timedelta(hours=1),
)
billingsvc_default_kube_kwargs = dict(
    image='591756927972.dkr.ecr.ap-northeast-1.amazonaws.com/billingsvc:{env}'.format(env=env),
    image_pull_policy='Always',  # Always pull image to reflect the changes in prod tag
    namespace='airflow',  # We should run in service namespace after airflow migration to service cluster
    in_cluster=True,
    is_delete_operator_pod=True,
    get_logs=True,
    configmaps=['billingsvc-{env}-config'.format(env=env)],
    resources=resources,
    execution_timeout=timedelta(minutes=20),
)

update_adn_report_task = CustomKubernetesPodOperator(
    task_id="update_adnetwork_report",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "update_adnetwork_report",
        "{{ execution_date }}",
        "{{ next_execution_date }}",
    ],
    name="import-update-adnetwork-report",
    **statssvc_default_kube_kwargs,
)

recalculate_payout = CustomKubernetesPodOperator(
    task_id="recalculate_payout",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "recalculate_payout",
    ],
    name="recalculate-payout",
    **billingsvc_default_kube_kwargs,
)

recalculate_performance = CustomKubernetesPodOperator(
    task_id="recalculate_performance",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "recalculate_performance",
    ],
    name="recalculate-performance",
    **billingsvc_default_kube_kwargs,
)

update_adn_report_task >> [recalculate_payout, recalculate_performance]
