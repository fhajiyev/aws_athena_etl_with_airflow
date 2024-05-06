from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
SLACK_CONN_ID = 'slack'


# ## Slack Channels ## #
# How to find your channel ID : https://www.wikihow.com/Find-a-Channel-ID-on-Slack-on-PC-or-Mac
SLACK_CHANNEL_MAP = dict({
    'data-emergency': 'CGZH7LYAG',
    'data-warning': 'C011DC66YUB',
    'dev-emergency': 'C60PE7K2M',
    'dev-emergency-statssvc': 'C012HCJ1S3T',
    'mission-mugshot': 'CNZ35Q6LE',
    'dev-emergency-mugshot': 'C017YK0N8EM',
    'data-emergency-oracle': 'C019TGSLMUN',
    'airflow-check-monitoring': 'C01FJ5A0PPD',
})


def task_sla_miss_slack_alert(context, channel='data-emergency'):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: SLA Missed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
        """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        channel=SLACK_CHANNEL_MAP[channel],
    )
    return failed_alert.execute(context=context)


def task_fail_slack_alert(context, channel='data-emergency'):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
        """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        channel=SLACK_CHANNEL_MAP[channel],
    )
    return failed_alert.execute(context=context)


def task_retry_slack_alert(context, channel='data-warning'):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :hammer_and_wrench: Task Retried.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
        """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        channel=SLACK_CHANNEL_MAP[channel],
    )
    return failed_alert.execute(context=context)
