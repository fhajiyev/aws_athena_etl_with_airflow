from functools import partial

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

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
    'start_date': datetime(2020, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(task_fail_slack_alert, channel=statssvc_alert_channel_id),
    'on_retry_callback': partial(task_retry_slack_alert, channel=statssvc_alert_channel_id),
    'task_concurrency': 10,
}

resources = {
    'request_memory': '2Gi',
    'limit_memory': '2Gi',
    'request_ephemeral_storage': '100Mi',
    'limit_ephemeral_storage': '100Mi',
}

dag = DAG(
    'statssvc_import_job',
    default_args=default_args,
    description='A job that creates stats to statssvc then updates report in billingsvc',
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
start_date_1day_delay = "{{execution_date - macros.timedelta(hours=24)}}"
end_date_1day_delay = "{{next_execution_date - macros.timedelta(hours=24, seconds=1)}}"
start_date_1day_6hrs_delay = "{{execution_date - macros.timedelta(days=1, hours=6)}}"
end_date_1day_6hrs_delay = "{{next_execution_date - macros.timedelta(days=1, hours=6, seconds=1)}}"
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

check_unit_ad_reward = ExternalTaskSensor(
    task_id='check_athena_process_statssvc_g_unit_ad_reward',
    external_dag_id='athena_process_statssvc_g_unit_ad_reward',
    execution_delta=0,
    dag=dag,
    timeout=60 * 30,        # 정각단위로 도는 dag가 많고, dag dependency가 많이 물려있어서 20분보다 오래걸리는 경우가 있음, 
                            # TODO k8s executer 도입 + task를 밀어넣는 scheduler의 주기를 짧게 만들기
    check_existence=True,
)

unit_ad_reward_task = CustomKubernetesPodOperator(
    task_id="unit_ad_reward",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_ad_reward",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-ad-reward",
    **statssvc_default_kube_kwargs,
)

unit_creative_task = CustomKubernetesPodOperator(
    task_id="unit_creative",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_creative",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-creative",
    **statssvc_default_kube_kwargs,
)

unit_creative_day_task = CustomKubernetesPodOperator(
    task_id="unit_creative_day",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_creative_day",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-creative-day",
    **statssvc_default_kube_kwargs,
)

adn_report_task = CustomKubernetesPodOperator(
    task_id="adn_report",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "adnetwork_report",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-adn-report",
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

creative_set_hour_task = CustomKubernetesPodOperator(
    task_id="creative_set_hour",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "creative_set_hour",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-creative-set-hour",
    **statssvc_default_kube_kwargs,
)

creative_set_day_task = CustomKubernetesPodOperator(
    task_id="creative_set_day",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "creative_set_day",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-creative-set-day",
    **statssvc_default_kube_kwargs,
)

unit_user_task = CustomKubernetesPodOperator(
    task_id="unit_user",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_user",
        start_date_1day_delay,
        end_date_1day_delay,
        delete_before_create,
    ],
    name="import-unit-user",
    **statssvc_default_kube_kwargs,
)

app_user_task = CustomKubernetesPodOperator(
    task_id="app_user",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "app_user",
        start_date_1day_delay,
        end_date_1day_delay,
        delete_before_create,
    ],
    name="import-app-user",
    **statssvc_default_kube_kwargs,
)

unit_dna_task = CustomKubernetesPodOperator(
    task_id="unit_dna",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_dna",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-dna",
    **statssvc_default_kube_kwargs,
)

unit_playtime_task = CustomKubernetesPodOperator(
    task_id="unit_playtime",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_playtime",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-playtime",
    **statssvc_default_kube_kwargs
)

adgroup_day_task = CustomKubernetesPodOperator(
    task_id="adgroup_day",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "adgroup_day",
        start_date_1day_6hrs_delay,
        end_date_1day_6hrs_delay,
        delete_before_create,
    ],
    name="import-adgroup-day",
    **statssvc_default_kube_kwargs
)

adgroup_total_task = CustomKubernetesPodOperator(
    task_id="adgroup_total",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "adgroup_total",
        start_date_1day_6hrs_delay,
        end_date_1day_6hrs_delay,
        delete_before_create,
    ],
    name="import-adgroup-total",
    **statssvc_default_kube_kwargs
)

unit_page_view_task = CustomKubernetesPodOperator(
    task_id="unit_page_view",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "unit_page_view",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-unit-page-view",
    **statssvc_default_kube_kwargs
)

lineitem_hour_task = CustomKubernetesPodOperator(
    task_id="lineitem_hour",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "lineitem_hour",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-lineitem-hour",
    **statssvc_default_kube_kwargs
)

lineitem_day_task = CustomKubernetesPodOperator(
    task_id="lineitem_day",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "lineitem_day",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-lineitem-day",
    **statssvc_default_kube_kwargs
)

lineitem_total_task = CustomKubernetesPodOperator(
    task_id="lineitem_total",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "lineitem_total",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-lineitem-total",
    **statssvc_default_kube_kwargs
)

creative_hour_task = CustomKubernetesPodOperator(
    task_id="creative_hour",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "creative_hour",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-creative-hour",
    **statssvc_default_kube_kwargs
)

creative_day_task = CustomKubernetesPodOperator(
    task_id="creative_day",
    dag=dag,
    cmds=[
        "python",
        "manage.py",
        "import_job",
        "creative_day",
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-creative-day",
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
        start_date_6hrs_delay,
        end_date_6hrs_delay,
        delete_before_create,
    ],
    name="import-creative-total",
    **statssvc_default_kube_kwargs
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
check_unit_ad_reward >> unit_ad_reward_task
[unit_point_task, unit_creative_task, adn_report_task, unit_ad_reward_task] >> unit_finance_task
unit_finance_task >> extend_unit_contract >> [recalculate_payout, recalculate_performance]
[unit_campaign_task, unit_creative_task] >> unit_inventory_task
unit_creative_task >> [lineitem_total_task, creative_total_task, unit_creative_day_task]
