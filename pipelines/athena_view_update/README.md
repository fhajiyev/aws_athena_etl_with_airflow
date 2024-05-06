# AthenaViewUpdate Pipeline
The AthenaViewUpdate DAG is used to verify the non-zero size of a newly generated partition and update a corresponding view to point at that partition.

**Important: Make sure to leave a comment in the YAML config about what the query is checking for and for which reason the check was implemented.**


Some usecase of this DAG includes:
1. Segmentsvc evaluation logic does not need to be aware of the most recent property partition and should use a view instead. The view therefore needs to be updated whenever a new property snapshot is generated. 


## Rendered Variables

#

| Parameter | Description | Example |
| --- | --- | --- |
| `{env}` | Environment | `dev`, `prod` |
| `{execution_date}` | Execution date of running DAG | `2020-09-08 00:00:00` |
| `{curr_time}` | Basically the same as `{execution_date}` | `2020-09-08 00:00:00` |


#

## General configuration

| Parameter | Description | Optional | Default |
| --- | --- | --- | --- |
| `pipeline_key` | Key to identify pipeline within each `pipeline_type`. Should follow the convention `addend_datasource`**_**`addend_query_desc`**__**`augend_data`**_**`augend_query_desc`**__**`short_desc`. (E.g. `redshift_ba_lineitem_all__athena_prod_buzzad_impression_all__compare_count`)  | `False` | |```
| `pipeline_type` | Type of the pipeline. Should be `athena_view_update` in this case | `False` | |

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
| `upstream_dependencies.dag_id` | The upstream DAG's id | `False` | | |
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
| `athena.database` | Athena database with view for which the update task needs to be performed | `False` | | |
| `athena.table` | Athena table with view for which the update task needs to be performed | `False` | | |
| `athena.output_bucket` | S3 bucket into which the query_count result is loaded  | `False` | | |
| `athena.output_prefix` | S3 prefix into which the query_count result is loaded  | `False` | | |
| `athena.query_count` | Query for checking the size of a newly generated partition | `False` | | |
| `athena.query_view` | Query for updating the view | `False` | | |
