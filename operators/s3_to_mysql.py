import shutil
import gzip
import os
import time

from typing import List

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.decorators import apply_defaults


class S3ToMySqlOperator(BaseOperator):
    """
    Loads S3 gzipped c files into a MySQL table.
    All files should be gzipped json type. It downloads gzipped json files and decompresses.
    It uses INSERT INTO query with specified chunk size.
    If you want to use LOAD DATA LOCAL INFILE function, you can find it in MySQL provider library.

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str

    :param prefix: Prefix string to filters the objects whose name begin with such prefix. (templated)
    :type prefix: str

    :param mysql_table: The MySQL table into where the data will be sent.
    :type mysql_table: str

    :param target_fields: The fields which the table has.
    :type target_fields: List

    :param aws_conn_id: The S3 connection that contains the credentials to the S3 Bucket.
    :type aws_conn_id: str

    :param mysql_conn_id: The MySQL connection that contains the credentials to the MySQL data base.
    :type mysql_conn_id: str
    """

    template_fields = ('bucket', 'prefix')
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 bucket: str,
                 prefix: str,
                 mysql_table: str,
                 target_fields: List,
                 chunk_size: int = 1000,
                 replace: bool = True,
                 aws_conn_id: str = 'aws_default',
                 mysql_conn_id: str = 'mysql_default',
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.target_fields = target_fields
        self.chunk_size = chunk_size
        self.replace = replace
        self.mysql_table = mysql_table
        self.aws_conn_id = aws_conn_id
        self.mysql_conn_id = mysql_conn_id

    def execute(self, context: dict) -> None:
        """
        Executes the transfer operation from S3 to MySQL.

        :param context: The context that is being provided when executing.
        :type context: dict
        """
        self.log.info('Loading %s to MySql table %s...', self.prefix, self.mysql_table)

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        files = s3_hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix,
            delimiter='/',
        )

        self.log.info('The number of files = {fcnt}'.format(fcnt=len(files)))

        for file in files:
            s3_file_name = '/'.join(['s3:/', self.bucket, file])
            self.log.info('---------- {file_name} is being loaded ----------'.format(file_name=s3_file_name))
            try:
                start_time = time.time()
                local_file_name = s3_hook.download_file(bucket_name=self.bucket, key=file)

                with gzip.open(local_file_name, 'rb') as f_in:
                    local_file_name_unzip = local_file_name + '_unzip'
                    with open(local_file_name_unzip, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                self.log.info("--- downloading & unzipping = %s seconds ---\n" % (time.time() - start_time))

                with open(local_file_name_unzip) as fp:
                    rows = []
                    cnt = 0
                    start_time = time.time()

                    for line in fp:
                        line = line.replace('null', 'None')
                        line_dict = eval(line)
                        rows.append(tuple(line_dict.values()))

                        if len(rows) == self.chunk_size:
                            self.log.info(
                                "--- preprocessing = %s seconds ---\n" % (time.time() - start_time))

                            insert_start_time = time.time()

                            cnt = cnt + 1
                            self.log.info('{cnt}-th chunk are being loaded.'.format(cnt=cnt))
                            mysql.insert_rows(
                                table=self.mysql_table,
                                rows=rows,
                                target_fields=self.target_fields,
                                replace=self.replace
                            )

                            self.log.info(
                                "--- inserting into mysql = %s seconds ---\n" % (time.time() - insert_start_time))

                            rows = []
                            start_time = time.time()

                    if len(rows) > 0:
                        cnt = cnt + 1
                        self.log.info('{cnt}-th chunk are being loaded.'.format(cnt=cnt))

                        mysql.insert_rows(
                            table=self.mysql_table,
                            rows=rows,
                            target_fields=self.target_fields,
                            replace=self.replace
                        )
                self.log.info('All files are loaded.')
            finally:
                os.remove(local_file_name)
                os.remove(local_file_name_unzip)



