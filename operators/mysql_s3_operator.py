import arrow
import csv
import datetime
import io
import json
import pandas as pd

from airflow.models import BaseOperator, SkipMixin
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.utils.decorators import apply_defaults
from utils.decorators import get_dag_run_uuid
from utils.constants import DEFAULT_MYSQL_BATCH_SIZE, DEFAULT_MYSQL_BATCH_PERIOD, MAX_MYSQL_BATCH_SIZE


class MySqlS3Operator(BaseOperator, SkipMixin):
    """
    Operator to extract rows from MySQL and store them in S3
    """

    template_fields = ('increment_key_from', 'increment_key_to', 's3_prefix', 's3_bucket')

    NO_OFFSET = 0

    RETRIEVE_MYSQL_INCREMENT_VALUES = """
        SELECT
            {increment_key}
        FROM
            `{table_name}`
        ORDER BY
            {increment_key} {order_by}
        LIMIT 1
    """

    RETRIEVE_MYSQL_VALUES_QUERY_STRING = """
        SELECT
            {columns}
        FROM
            `{table_name}`
        WHERE
            {increment_key} >= '{increment_key_from}' AND
            {increment_key} < '{increment_key_to}'
        LIMIT {batch_size} OFFSET {offset}
    """

    RETRIEVE_MYSQL_VALUES_EQ_INCREMENT_KEY_QUERY_STRING = """
        SELECT
            {columns}
        FROM
            `{table_name}`
        WHERE
            {increment_key} = '{increment_key_from}'
        LIMIT {batch_size} OFFSET {offset}
    """

    RETRIEVE_MYSQL_VALUES_DUMP_QUERY_STRING = """
        SELECT
            {columns}
        FROM
            `{table_name}`
    """

    @apply_defaults
    def __init__(
        self,
        mysql_conn_id,
        mysql_table_name,
        mysql_columns,
        increment_key,
        increment_key_type,
        s3_conn_id,
        s3_bucket,
        s3_prefix,

        mysql_database_name=None,
        increment_key_from_xcom=False,
        xcom_source_task_id=None,
        increment_key_from=None,
        increment_key_to=None,
        batch_period=None,
        batch_size=None,
        file_key=None,
        data_format='csv',
        skip_downstream=False,
        s3_prefix_append_dag_run_uuid=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.mysql_database_name = mysql_database_name
        self.mysql_table_name = mysql_table_name
        self.mysql_columns = mysql_columns
        self.increment_key = increment_key
        self.increment_key_type = increment_key_type
        self.increment_key_from_xcom = increment_key_from_xcom

        self.xcom_source_task_id = xcom_source_task_id
        self.increment_key_from = increment_key_from
        self.increment_key_to = increment_key_to
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_prefix_append_dag_run_uuid = s3_prefix_append_dag_run_uuid
        self.batch_period = batch_period or DEFAULT_MYSQL_BATCH_PERIOD
        self.batch_size = batch_size or DEFAULT_MYSQL_BATCH_SIZE
        self.file_key = file_key or mysql_table_name
        self.data_format = data_format

        self.skip_downstream = skip_downstream
        self.dag_run_uuid = None

        if self.increment_key_type.lower() not in ("timestamp", "numeric", "dump"):
            raise Exception("Please select the increment_key_type from  'timestamp', 'numeric', 'dump' ")

    def _get_increment_key_from_xcom(self, context):
        task_instance = context['task_instance']
        xcom_dict = task_instance.xcom_pull(task_ids=self.xcom_source_task_id, key='last_increment_value')
        if xcom_dict is not None:
            self.increment_key_from = xcom_dict['last_increment_value']
            self.log.info("Retrieving increment value {0} from xcom".format(self.increment_key_from))
        else:
            self.increment_key_from = None
            self.log.info("Cannot retrieve increment value from xcom.")

    def _run_query(self, query_string):
        self.log.info('Executing: %s', query_string)
        self.hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.mysql_database_name)
        result = self.hook.get_records(query_string)
        return result

    def _convert_parquet(self, columns, rows):
        bytes_io = io.BytesIO()
        df = pd.DataFrame.from_records(data=rows, columns=columns)
        # allowed_truncated_timestamps allows ns, us timestamps to be truncated to milliseconds
        df.to_parquet(bytes_io, engine='pyarrow', compression='snappy', allow_truncated_timestamps=True)
        bytes_io.seek(0)
        return bytes_io

    def _convert_json(self, columns, rows):
        str_io = io.StringIO()

        for row in rows:
            json.dump(dict(zip(columns, row)), str_io, default=str)
            str_io.write('\n')
        bytes_io = io.BytesIO(str_io.getvalue().encode('utf-8'))
        bytes_io.seek(0)

        return bytes_io

    def _convert_csv(self, columns, rows):
        str_io = io.StringIO()
        writer = csv.writer(str_io,
                            dialect=csv.excel,
                            delimiter=str('|'),
                            escapechar='\\',
                            quotechar='\"',
                            doublequote=False,
                            skipinitialspace=False,
                            quoting=csv.QUOTE_ALL,)
        # Write header
        writer.writerow(columns)
        # Write rows
        for row in rows:
            data = []
            for field in row:
                if isinstance(field, datetime.datetime):
                    data.append(field.isoformat(timespec='microseconds', sep=' '))
                elif isinstance(field, str):
                    data.append(field.replace('\n', '\\n').replace('\\', '\\\\'))
                else:
                    data.append(field)
            writer.writerow(data)
        bytes_io = io.BytesIO(str_io.getvalue().encode('utf-8'))
        bytes_io.seek(0)

        return bytes_io

    def _get_mysql_table_increment_key_range(self, order_by):
        """
        :param order_by: name of the increment_key to retrieve ranges from
        """

        res = self._run_query(
            query_string=self.RETRIEVE_MYSQL_INCREMENT_VALUES.format(
                table_name=self.mysql_table_name,
                increment_key=self.increment_key,
                order_by=order_by
            )
        )

        if len(res) >= 0:
            return res[0][0]
        else:
            return None

    def _retrieve_mysql_values(self, increment_key_from, increment_key_to, batch_size, offset, ):
        """
        :param increment_key_from: Starting value of the increment key to query data
        :param increment_key_to: Ending value of the increment key to query data
        :param batch_size: Batch size of each retrieval
        :param offset: SQL offset to divide query results into reasonably sized batches
        """

        self.log.info(
            "Retrieving MySQL values from {0} to {1} with batch_size {2}".format(increment_key_from, increment_key_to,
                                                                                 batch_size))

        columns = self.mysql_columns

        if increment_key_from and increment_key_to:
            if increment_key_from < increment_key_to:
                results = self._run_query(
                    query_string=self.RETRIEVE_MYSQL_VALUES_QUERY_STRING.format(
                        table_name=self.mysql_table_name,
                        columns=', '.join(self.mysql_columns),
                        increment_key=self.increment_key,
                        increment_key_from=increment_key_from,
                        increment_key_to=increment_key_to,
                        batch_size=self.batch_size,
                        offset=offset,
                    )
                )
            elif increment_key_from == increment_key_to:
                results = self._run_query(
                    query_string=self.RETRIEVE_MYSQL_VALUES_EQ_INCREMENT_KEY_QUERY_STRING.format(
                        table_name=self.mysql_table_name,
                        columns=', '.join(self.mysql_columns),
                        increment_key=self.increment_key,
                        increment_key_from=increment_key_from,
                        batch_size=self.batch_size,
                        offset=offset,
                    )
                )
            else:
                raise Exception(f"increment_key_to : {increment_key_to} is less than increment_key_from : {increment_key_from}")
        else:
            results = self._run_query(
                query_string=self.RETRIEVE_MYSQL_VALUES_DUMP_QUERY_STRING.format(
                    table_name=self.mysql_table_name,
                    columns=', '.join(self.mysql_columns),
                )
            )

        if len(results) > 0:
            self.log.info(f"There are {len(results)} rows to convert")
            if self.data_format == 'csv':
                return self._convert_csv(columns=columns, rows=results)
            elif self.data_format == 'json':
                return self._convert_json(columns=columns, rows=results)
            elif self.data_format == 'parquet':
                return self._convert_parquet(columns=columns, rows=results)
        else:
            return None

    def _upload_file_to_s3(self, file_key, file_to_upload):
        file_url = '/'.join([self.s3_prefix, '.'.join([file_key, self.data_format]), ])
        self.s3_conn.load_file_obj(
            file_obj=file_to_upload,
            key=file_url,
            bucket_name=self.s3_bucket,
            replace=True,
        )
        self.log.info("Uploaded csv file to {0}".format(file_url))

    def _unload_to_s3_dump(self):
        file_to_upload = self._retrieve_mysql_values(
            increment_key_from=None,
            increment_key_to=None,
            batch_size=MAX_MYSQL_BATCH_SIZE,
            offset=self.NO_OFFSET,
        )
        if file_to_upload is not None:
            self._upload_file_to_s3(file_key='-'.join([self.file_key, str(self.increment_key_from), str(self.increment_key_to)]), file_to_upload=file_to_upload)
            return True
        else:
            self.log.info("There were no csv files to upload")
            return False

    def _unload_to_s3_numeric(self, ):
        cum_csv_count = 0
        for increment_start in range(self.increment_key_from, self.increment_key_to, self.batch_size):
            file_to_upload = self._retrieve_mysql_values(
                increment_key_from=increment_start,
                increment_key_to=increment_start + self.batch_size,
                batch_size=self.batch_size,
                offset=self.NO_OFFSET,
            )
            if file_to_upload is not None:
                self._upload_file_to_s3(file_key='-'.join([self.file_key, str(increment_start)]), file_to_upload=file_to_upload)
                cum_csv_count += 1
        if cum_csv_count > 0:
            return True
        else:
            self.log.info("There were no csv files to upload")
            return False

    def _unload_to_s3_timestamp(self, ):
        start_time = arrow.get(self.increment_key_from)
        end_time = arrow.get(self.increment_key_to)
        cum_csv_count = 0

        for sub_start_time in arrow.Arrow.range(self.batch_period, start_time, end_time):
            # Note : for arrow.shift() we need to pluralize batch_period
            sub_end_time = min(sub_start_time.shift(**{f'{self.batch_period}s': 1}), end_time)
            offset = 0
            while True:
                file_to_upload = self._retrieve_mysql_values(
                    increment_key_from=sub_start_time.format('YYYY-MM-DD HH:mm:ss'),
                    increment_key_to=sub_end_time.format('YYYY-MM-DD HH:mm:ss'),
                    batch_size=self.batch_size,
                    offset=offset,
                )

                offset += self.batch_size

                if file_to_upload is not None:
                    self._upload_file_to_s3(file_key='-'.join([self.file_key, sub_start_time.format('YYYY-MM-DD-HH'), str(offset)]), file_to_upload=file_to_upload)
                    cum_csv_count += 1
                else:
                    break

        if cum_csv_count > 0:
            return True
        else:
            self.log.info("There were no files to upload")
            self.skip_downstream = True
            return False

    def _unload_to_s3(self, ):
        if self.increment_key_type == 'dump':
            return self._unload_to_s3_dump()
        elif self.increment_key_type == 'numeric':
            return self._unload_to_s3_numeric()
        elif self.increment_key_type == 'timestamp':
            return self._unload_to_s3_timestamp()
        else:
            raise Exception("increment_key_type is not valid. Should be 'dump', numeric' or 'timestamp' ")

    @get_dag_run_uuid
    def execute(self, context):
        self.s3_conn = S3Hook(aws_conn_id=self.s3_conn_id)

        if self.increment_key_from_xcom is True:
            self.log.info("Retrieving increment_key_from value from Xcom")
            self._get_increment_key_from_xcom(context=context)
        if not self.increment_key_from:
            self.log.info("Retrieving increment_key_from value from MySQL source")
            self.increment_key_from = self._get_mysql_table_increment_key_range(order_by='ASC')
        if not self.increment_key_to:
            self.log.info("Retrieving increment_key_to value from MySQL source")
            self.increment_key_to = self._get_mysql_table_increment_key_range(order_by='DESC')
        if self.s3_prefix_append_dag_run_uuid is True:
            self.s3_prefix += '/' + self.dag_run_uuid

        upload_success = self._unload_to_s3()

        if upload_success is True:
            return self.dag_run_uuid
        elif self.skip_downstream is True:
            self.log.info('Skipping downstream tasks...')
            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            self.log.info("Downstream task_ids %s", downstream_tasks)
            if downstream_tasks:
                self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
            self.log.info("Done skipping downstream tasks.")
            return None
        else:
            raise Exception("Nothing to upload, yet not skipping downstream tasks")
