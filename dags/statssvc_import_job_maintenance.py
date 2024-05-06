from functools import partial

from airflow import DAG
from datetime import datetime, timedelta
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
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 7),
    'end_date': datetime(2020, 11, 11),
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
    'statssvc_import_job_maintenance',
    default_args=default_args,
    description='temporary dag for maintenance of check_point_integrity of statssvc_import_job',
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
# Delay all jobs 4 hours from execution time - that's when all data are available for import.
start_date = "{{execution_date}}"
end_date = "{{next_execution_date - macros.timedelta(seconds=1)}}"
start_date_6hrs_delay = "{{execution_date - macros.timedelta(hours=6)}}"
end_date_6hrs_delay = "{{next_execution_date - macros.timedelta(hours=6, seconds=1)}}"
delete_before_create = "--delete_before_create=1"  # Turn this flag to override existing data

check_point_integrity = CustomKubernetesPodOperator(
    task_id="check_point_integrity",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "check_point_integrity",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
    ],
    name="check-point-integrity",
    **statssvc_default_kube_kwargs,
)

unit_point_task = CustomKubernetesPodOperator(
    task_id="unit_point",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_point",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-point",
    **statssvc_default_kube_kwargs,
)

unit_campaign_task = CustomKubernetesPodOperator(
    task_id="unit_campaign",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_campaign",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-campaign",
    **statssvc_default_kube_kwargs,
)

unit_finance_task = CustomKubernetesPodOperator(
    task_id="unit_finance",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_finance",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="generate-unit-finance",
    **statssvc_default_kube_kwargs,
)

unit_inventory_task = CustomKubernetesPodOperator(
    task_id="unit_inventory",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_inventory",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="generate-unit-inventory",
    **statssvc_default_kube_kwargs,
)

extend_unit_contract = CustomKubernetesPodOperator(
    task_id="extend_unit_contract",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "extend_unit_contract",
        start_date_6hrs_delay,
    ],
    name="extend-unit-contract",
    **billingsvc_default_kube_kwargs,
)

recalculate_payout = CustomKubernetesPodOperator(
    task_id="recalculate_payout",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "recalculate_payout",
        "--start_date={}".format(start_date_6hrs_delay),
        "--end_date={}".format(end_date_6hrs_delay),
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
        "--start_date={}".format(start_date_6hrs_delay),
        "--end_date={}".format(end_date_6hrs_delay),
    ],
    name="recalculate-performance",
    **billingsvc_default_kube_kwargs,
)

check_point_integrity >> [unit_point_task, unit_campaign_task]
unit_point_task >> unit_finance_task
unit_finance_task >> extend_unit_contract >> [recalculate_payout, recalculate_performance]
unit_campaign_task >> unit_inventory_task
