# AthenaDeduplicate Pipeline

## Rendered Variables

#

| Parameter | Description | Example |
| --- | --- | --- |
| `{env}` | Environment | `dev`, `prod` |
| `{execution_date}` | Execution date of running DAG | `2020-09-08 00:00:00` |
| `{next_execution_date}` | Next execution date of running DAG | `2020-09-08 01:00:00` |
| `{year}` | Year of `{execution_date}` | `2020` |
| `{month}` | Month of `{execution_date}` | `09` |
| `{day}` | Day of `{execution_date}` | `08` |
| `{hour}` | Hour of `{execution_date}` | `00` |

#

## General configuration

| Parameter | Description | Optional | Default |
| --- | --- | --- | --- |
| `pipeline_key` | Key to identify pipeline within each `pipeline_type` | `False` | |
| `pipeline_type` | Type of the pipeline. Should be `athena_deduplicate` in this case | `False` | |

#
__Airflow DAG config values(Optional):__

| Parameter | Description | Optional | Default |
| --- | --- | --- | --- |
| `pipeline_dag_configs.` | Dictionary of configurations that applies to the DAG itself | `True` |
| `pipeline_dag_configs.start_date` | From which execution date the DAG should end scheduling | `True` |
| `pipeline_dag_configs.end_date` | From which execution date the DAG should start scheduling | `True` |
| `pipeline_dag_configs.***` | Refer to [source code](https://github.com/apache/airflow/blob/master/airflow/models/dag.py) or ask @datavil on slack| `True` |

#
## Dependency configuration (Optional)
#
__Upstream dependency values:__
| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `upstream_dependencies.dag_id` | The upstream DAG's id | `False` |
| `upstream_dependencies.timedelta_days` | The number of days the `execution_date` of the upstream DAG run is apart from | `True` | `0` | `-1, -2, 1, 2 ...` |
| `upstream_dependencies.timedelta_hours` | The number of hours the `execution_date` of the upstream DAG run is apart from | `True` | `0` | `-1, -2, 1, 2 ...` |
#

__Downstream dependency values:__
| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `downstream_dependencies.dag_id` | The dag_id of the downstream DAG | `False` |
| `downstream_dependencies.task_id` | The downstream DAG's task_id | `False` | | `generate_uuid, ...`|
#
## Alerts configuration (Optional)
#
| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `alerts.slack.trigger` | The task state at which the alert is delivered | `False` | | `failure, sla_miss, retry`|
| `alerts.slack.args.channel` | The target slack channel for the alert  | `False` | `failure=data-emergency, retry= data-warning, sla_miss=data-emergency`| Any channel that is registered at `utils.slack.SLACK_CHANNEL_MAP`|


#
## Pipeline configuration
#

| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `athena.query` | The create view query | `False` | | |
| `athena.database` | The Athena database the table should be created in | `False` | | |
| `athena.table` | The Athena table name | `False` | | |
