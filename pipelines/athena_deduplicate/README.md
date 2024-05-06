# AthenaDeduplicate Pipeline

## Rendered Variables

#

| Parameter | Description | Example |
| --- | --- | --- |
| `{env}` | Environment | `dev`, `prod` |
| `{execution_date}` | Execution date of running DAG | `2020-09-08 00:00:00` |
| `{next_execution_date}` | Next execution date of running DAG | `2020-09-08 01:00:00` |
| `{create_start_date}` | `{execution_date} - athena_deduplication.scan_days` | `2020-09-01 00:00:00` |
| `{create_end_date}` | `{next_execution_date} - athena_deduplication.scan_days` | `2020-09-01 01:00:00` |
| `{year}` | Year of `{create_start_date}` | `2020` |
| `{month}` | Month of `{create_start_date}` | `09` |
| `{day}` | Day of `{create_start_date}` | `01` |
| `{hour}` | Hour of `{create_start_date}` | `00` |

#

## General configuration

| Parameter | Description | Optional | Default |
| --- | --- | --- | --- |
| `pipeline_key` | Key to identify pipeline within each `pipeline_type` | `False` | |
| `pipeline_type` | Type of the pipeline. Should be `athena_deduplicate` in this case | `False` | |
| `skip_on_empty_query_result` | `check_s3_object_presence` will be marked to `failed` when the task finds no object for the output_location if `skip_on_empty_query_result: false`. Otherwise, the task will be marked to `skipped` then the dag will have `success` state. | `True` | `True` |

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
| `athena.deduplication.type` | Use `increased_number` if updated_field is used like version. `version_string` is used for updated_field | `False` | | `increased_number, version_string` |
| `athena.deduplication.scan_days` | Scanning range to get latest state in duplicated records | `False` | | |
| `athena.deduplication.original_table` | The Athena table with duplicated records | `False` | | |
| `athena.deduplication.fields` | Columns in the original table | `False` | | |
| `athena.deduplication.unique_fields` | Unique columns in the original table | `False` | | |
| `athena.deduplication.updated_field` | The column to identify latest record in the original table | `False` | | |
| `athena.deduplication.updated_values` | Required if `version_string` is used. First(or last) element is recognized lowest(or highest) version | `True` | | list of string |
| `athena.deduplication.bucketed_by` | Query output is spreaded to `bucketed_by` files. Recommend for output file size over 128MB. [reference](https://aws.amazon.com/ko/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena) | `True` | | |
| `athena.deduplication.bucket_count` | Split the query output to `bucket_count` files if `bucketed_by` field is speicified.| `True` | 10 | |
| `athena.output_bucket` | The Bucket where query output is stored | `False` | | |
| `athena.output_prefix` | The prefix where query output is stored | `False` | | |
| `athena.file_key` | Prefix of query output file | `False` | | |
| `athena.file_extension` | Extension of query output file, only `parquet` is supported | `False` | | `parquet` |
| `athena.create_table_syntax` | The table creation script template used for `reconcile_athena_table` task. | `False` | | |
| `athena.database` | The Athena database the table should be created in | `False` | | |
| `athena.table` | The Athena table name | `False` | | |
| `athena.location` | Base location for the athena table. This value fills in the `{location}` param in `athena.create_table_syntax` | `False` |
| `athena.partition.key` | Key for the partition | `False` | `partition_timestamp` | |
| `athena.partition.subdir` | Subdirectory under `athena.location` where the data for the partition exists | `False` | `year={year}/month={month}/day={day}/hour={hour}"`| `str`|
