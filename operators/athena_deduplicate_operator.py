# -*- coding: utf-8 -*-

import logging

from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.utils.decorators import apply_defaults


logger = logging.getLogger(__name__)


class AthenaDeduplicateOperator(AWSAthenaOperator):
    QUERY_TEMPLATE = """
        CREATE TABLE IF NOT EXISTS {database}.{temp_table}
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{output_bucket}/{output_prefix}'
            {bucketed}
        ) AS WITH dup_records AS (
            SELECT
                {case_mapping} {fields}
            FROM {database}.{original_table}
            WHERE
                partition_timestamp >= TIMESTAMP '{create_start_time}' - interval '1' hour
                AND partition_timestamp < TIMESTAMP '{end_time}'
                AND created_at >= TIMESTAMP '{create_start_time}'
                AND created_at < TIMESTAMP '{create_end_time}'
        ), dup_records_row_num AS (
            SELECT {fields},
                ROW_NUMBER() OVER (
                    PARTITION BY {select_unique_fields}
                    ORDER BY {updated_version_field} DESC, updated_at DESC
                ) as row_num
            from dup_records
        )
        SELECT
            {fields},
            DATE_TRUNC('hour', created_at) AS partition_timestamp
        FROM dup_records_row_num
        WHERE row_num = 1
    """
    BUCKETED_TEMPLATE = ", bucketed_by = ARRAY['{bucketed_by}'], bucket_count = {bucket_count}"
    DEFAULT_BUCKET_COUNT = 10

    @apply_defaults
    def __init__(
        self,
        create_start_time,
        create_end_time,
        end_time,
        database,
        original_table,
        temp_table,
        output_bucket,
        output_prefix,
        dedup_type,
        fields,
        unique_fields,
        updated_field,
        updated_values=None,
        bucketed_by=None,
        bucket_count=None,
        *args,
        **kwargs,
    ):
        self.query = self.__build_query(
            create_start_time,
            create_end_time,
            end_time,
            database,
            original_table,
            temp_table,
            output_bucket,
            output_prefix,
            dedup_type,
            fields,
            unique_fields,
            updated_field,
            updated_values,
            bucketed_by,
            bucket_count,
        )
        super(AthenaDeduplicateOperator, self).__init__(
            query=self.query,
            database=database,
            *args,
            **kwargs
        )

    def pre_execute(self, context):
        self.log.info(self.query)

    def __build_query(
        self,
        create_start_time,
        create_end_time,
        end_time,
        database,
        original_table,
        temp_table,
        output_bucket,
        output_prefix,
        dedup_type,
        fields,
        unique_fields,
        updated_field,
        updated_values=None,
        bucketed_by=None,
        bucket_count=None,
    ):
        # Use date_trunc(hour, created_at) as partition_timestamp
        if 'partition_timestamp' in fields:
            fields.remove('partition_timestamp')

        bucketed = ""
        if bucketed_by is not None:
            bucketed = self.BUCKETED_TEMPLATE.format(
                bucketed_by=bucketed_by,
                bucket_count=bucket_count or self.DEFAULT_BUCKET_COUNT,
            )

        if dedup_type == 'version_string':
            updated_version_field = 'record_updated_version'

            case_mapping = "CASE "
            for i, value in enumerate(updated_values):
                case_mapping += "WHEN {field} = '{value}' THEN {version} ".format(
                    field=updated_field,
                    value=value,
                    version=i,
                )
            case_mapping += "ELSE 0 END AS {},".format(updated_version_field)

            return self.QUERY_TEMPLATE.format(
                create_start_time=create_start_time,
                end_time=end_time,
                create_end_time=create_end_time,
                database=database,
                original_table=original_table,
                temp_table=temp_table,
                output_bucket=output_bucket,
                output_prefix=output_prefix,
                fields=self.__select_fields(fields),
                case_mapping=case_mapping,
                select_unique_fields=self.__select_fields(unique_fields),
                bucketed=bucketed,
                updated_field=updated_field,
                updated_version_field=updated_version_field,
            )
        elif dedup_type == 'increased_number':
            return self.QUERY_TEMPLATE.format(
                create_start_time=create_start_time,
                end_time=end_time,
                create_end_time=create_end_time,
                database=database,
                original_table=original_table,
                temp_table=temp_table,
                output_bucket=output_bucket,
                output_prefix=output_prefix,
                fields=self.__select_fields(fields),
                case_mapping='',
                select_unique_fields=self.__select_fields(unique_fields),
                bucketed=bucketed,
                updated_field=updated_field,
                updated_version_field=updated_field,
            )

        raise Exception(f'unsupported dedup_type {dedup_type}')

    def __select_fields(self, fields):
        q = ""
        for c in fields:
            q += "{}, ".format(c)
        return q[:-2] # 마지막 컴마 제거
