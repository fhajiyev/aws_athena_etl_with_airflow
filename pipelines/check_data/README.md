# CheckData Pipeline
The CheckData DAGs provides a generalized way to compare a first order linear combination of one or two numeric metrics—
whether they be queried from a DataSource or provided as a parameter—against a numeric value and "fail" when the criteria is not met.
Note that each data source query should return a single numerical value as a result. `(i.e SELECT COUNT(*) FROM ba_unit)`

**Important: Make sure to leave a comment in the YAML config about what the query is checking for and for which reason the check was implemented.**


The comparison looks like below

`a * augend + b * addend ( < <= = >= > ) threshold`

Some usecase of this DAG includes:
1. Sanity checking data pipelines that loads data into Redshift hourly.
3. Comparing results from an Athena data query with Redshift data query (TBD)


## Rendered Variables

#

| Parameter | Description | Example |
| --- | --- | --- |
| `{env}` | Environment | `dev`, `prod` |
| `{execution_date}` | Execution date of running DAG | `2020-09-08 00:00:00` |
| `{next_execution_date}` | Next execution date of running DAG | `2020-09-08 01:00:00` |
| `{start_time}` | Basically the same as `{execution_date}` | `2020-09-01 00:00:00` |
| `{end_time}` | Basically the same as `{next_execution_date}` | `2020-09-01 01:00:00` |

#

## General configuration

| Parameter | Description | Optional | Default |
| --- | --- | --- | --- |
| `pipeline_key` | Key to identify pipeline within each `pipeline_type`. Should follow the convention `addend_datasource`**_**`addend_query_desc`**__**`augend_data`**_**`augend_query_desc`**__**`short_desc`. (E.g. `redshift_ba_lineitem_all__athena_prod_buzzad_impression_all__compare_count`)  | `False` | |```
| `pipeline_type` | Type of the pipeline. Should be `check_data` in this case | `False` | |
| `execution_delay` | Seconds to wait before running dag | `True` | |

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

__Augend(left hand side of the operator) configuration(Required):__

_Addend(right hand side of the operator) configuration(Optional) is same with Augend_

| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `augend.data_source.type` | The type of the data source to use for the left hand side | `False` | | `redshift, athena, numeric_zero, TBD(mysql, Pure)` |
| `augend.query` | The query against the data source. Refer to the rendered variables for templated variables. **Note that the query result must be a single, numeric value.** | `True` | | |
| `augend.multiplier` | The multiplier `a` to be applied to `augend`'s value  | `True` | | |
| `augend.default` | Set default value if no query result exists  | `True` | | |

__Pipeline configurations for Redshift__
| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `augend.data_source.type` | `redshift` | `True` | | `redshift` |
| `augend.data_source.conn_id` | The `conn_id` of the data_source connection | `True` | | |

__Pipeline configurations for Athena__
| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `augend.data_source.type` | `athena` | `False` | | `athena` |
| `augend.data_source.database` | The Athena database the table should be created in | `False` | | |
| `augend.data_source.workgroup` | The workgroup is used to classify queries into each service to track heavy query in athena. Use the service name that the query read (e.g. rewardsvc, buzzscreen) | `False` | |  |

__Comparison Operator configuration(Required):__

| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `comparison.operator` | The mathematical comparison operator to compare `augend + addend` with `threshold`. Should be one of `lt, le, eq, ge, gt`. Refer to [link](https://docs.python.org/3/library/operator.html) for details | `False` | | `lt, le, eq, ge, gt` |
| `comparison.threshold` | The threshold to be compared with `augend + addend`.  | `False` | | Any floating point value `0.0`  |
