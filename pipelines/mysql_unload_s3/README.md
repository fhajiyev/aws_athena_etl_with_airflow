# MySQLS3Unload Pipeline


## General configuration

| Parameter | Description | Optional |
| --- | --- | --- |
| `pipeline_key` | Key to identify pipeline within each `pipeline_type` | `False` |
| `pipeline_type` | Type of the pipeline. Should be `mysql_s3_unload` in this case | `False` |

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

__MySQL values:__

| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `mysql.conn_id` | The MySQL connection_id for the source DB | `False` | |
| `mysql.table_name` | Table name of the source | `False` | | |
| `mysql.increment_key` | The column to use as range index. **Be sure to check whether that the column is an indexed column** | `False` | | |
| `mysql.increment_key_type` | Data type of the increment_key | `False` | | `numeric, timestamp, dump` |
| `mysql.fields` | Columns to query data for | `False` | |
| `mysql.batch_size` | The batch_size of a single data extract query sent to MySQL | `True` | `1000`| `second, minute, hour, day, month, ... ` Can be any datetime property |
| `mysql.batch_period` | (Only applies to `increment_key_type=timestamp`) The datetime range  | `True` | `hour` | `seconds, minutes, hours, days, months, ... ` Can be any datetime property |

#
__S3 values:__

| Parameter | Description | Optional | Acceptable values |
| --- | --- | --- | --- |
| `s3.bucket` | The destination s3 bucket of the extracted data | `False` | `"{{ var.value.get('server_env', 'prod') }}-buzzvil-data-lake"` |
| `s3.prefix` | The destination s3 prefix of the extracted data | `False` |
| `s3.file_key` | The file key (not including the extension) of the resulting | `False` |
| `s3.data_format` | The data format of the extract | `False` | `json, parquet, csv` |


#
__Athena values: (Optional: If you require Athena/Glue Cataloging of the unloaded data)__

| Parameter | Description | Optional | Default | Acceptable values |
| --- | --- | --- | --- | --- |
| `athena.create_table_syntax` | The table creation script template used for `reconcile_athena_table` task | `False` | | |
| `athena.database` | The Athena database the table should be created in | `False` | | |
| `athena.table` | The Athena table name | `False` | | |
| `athena.location` | Base location for the athena table. This value fills in the `{location}` param in `athena.create_table_syntax` | `False` |
| `athena.partition.key` | Key for the partition | `False` | | |
| `athena.partition.value` | Value for the partition | `True` | `{{execution_date.strftime('%Y-%m-%d %H:00:00')}}` | `str`
| `athena.partition.subdir` | Subdirectory under `athena.location` where the data for the partition exists | `True` | `"year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/day={{ execution_date.strftime('%d') }}/hour={{ execution_date.strftime('%H') }}"`| `str`|
