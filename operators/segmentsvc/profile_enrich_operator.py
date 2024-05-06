from __future__ import annotations

import enum
import io
import os
import pandas as pd
import urllib

from airflow.contrib.hooks.grpc_hook import GrpcHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from ast import literal_eval
from dataclasses import dataclass
from pandas.io.json import json_normalize
from typing import Callable
from utils import grpc_mappers, grpc_stubs
from utils.decorators import get_dag_run_uuid



@dataclass
class DataSource:
    class ProfileIdentifier(enum.Enum):
        IFA = 'ifa'
        APP_ID = 'app_id'
        PUBLISHER_USER_ID = 'publisher_user_id'
        ACCOUNT_ID = 'account_id'
        COOKIE_ID = 'cookie_id'

    class SourceType(enum.Enum):
        EVENT = 'event'
        TAG = 'tag'
        PROPERTY = 'property'

    ENRICHED_SCHEMA_MAPPING = {
        SourceType.EVENT: ['data_source_id', 'event_type', 'event_timestamp', 'profile_id', 'payload'],
        SourceType.TAG: ['data_source_id', 'tag_key', 'tag_value', 'update_timestamp', 'profile_id', 'payload']
        # 'property'
    }

    profile_identifier_list: list(ProfileIdentifier)
    source_type: SourceType

    def get_schema_mapping(self, ):
        return self.ENRICHED_SCHEMA_MAPPING.get(self.source_type)


class ProfileEnrichOperator(BaseOperator):
    """
    !! Note that this operator is to be used solely for/by segmentsvc for now !!
    1. Queries the xcom for s3 object location of the source data
    2. Calls profilesvc.ListProfileIDs with ProfileIdentifiers of the source data
    3. Merges the original data with profile dimensions fetched from profilesvc
    s3_conn_id:
    """
    template_fields = ('mapper_func', 'stub_class_func', 'call_func')

    @apply_defaults
    def __init__(self, s3_conn_id: str, grpc_conn_id: str, message_source_task_id: str,
                 mapper_func: Callable, stub_class_func: Callable, call_func: Callable,
                 data_source: DataSource, profilesvc_req_batch_size: int, data_merge_batch_size: int,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.grpc_conn_id = grpc_conn_id
        self.message_source_task_id = message_source_task_id

        self.mapper_func = mapper_func
        self.stub_class_func = stub_class_func
        self.call_func = call_func

        self.data_source = data_source
        self.PROFILESVC_REQ_BATCH_SIZE = profilesvc_req_batch_size or 1000
        self.DATA_MERGE_BATCH_SIZE = data_merge_batch_size or 100000

        self.resources = {"cpus": 1, "ram": 1024}

    def _get_s3_hook(self, ):
        return S3Hook(self.s3_conn_id, )

    def _get_grpc_hook(self, ):
        return GrpcHook(self.grpc_conn_id, )

    def _process_xcom_messages(self, context, source_task: str) -> tuple(str, str, str):
        task_instance = context['task_instance']
        messages = task_instance.xcom_pull(task_ids=source_task, key='messages')
        if messages is not None:
            yield from [self._process_sqs_record(sqs_record=message) for message in messages['Messages']]
        else:
            self.log.info("No message to process")

    def _process_sqs_record(self, sqs_record, ) -> tuple(str, str, str):
        self.log.info("Processing SQS record")
        # Note that we are only processing single records within each task
        record = literal_eval(literal_eval(sqs_record['Body'])['Message'])['Records'][0]

        src_s3_bucket = record['s3']['bucket']['name']
        src_s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

        # Profile enriched data goes into gold zone
        dest_s3_key = src_s3_key.replace("segmentsvc/landing/", "segmentsvc/gold/", 1)

        return (src_s3_bucket, src_s3_key, dest_s3_key)

    def _read_events_from_s3(self, src_s3_bucket: str, src_s3_key: str, ) -> pd.DataFrame:
        self.log.info(f"Reading events {src_s3_bucket}/{src_s3_key}")
        s3_hook = self._get_s3_hook()
        event_file = s3_hook.get_key(bucket_name=src_s3_bucket, key=src_s3_key)
        #  TODO: Add parquet option if required
        # Currently, cannot read nested structures in PARQUET
        return pd.read_json(io.BytesIO(event_file.get()['Body'].read()), compression='gzip', lines=True)

    def _extract_profile_info(self, df_events: pd.DataFrame, ) -> tuple(pd.DataFrame, pd.DataFrame):
        # Normalize profile dimensions from events payload
        df_profile_dimensions = json_normalize(df_events['payload']).reindex(columns=self.data_source.profile_identifier_list)
        df_profile_dimensions.fillna('', inplace=True)
        self.log.info(df_profile_dimensions.head())
        df_events_exploded = pd.merge(df_events.reset_index(drop=True), df_profile_dimensions.reset_index(drop=True), sort=False, copy=False, left_index=True, right_index=True)
        self.log.info(df_events_exploded.head())
        self.log.info(f"Extracted profile_dimensions from {len(df_profile_dimensions)} events")
        return df_profile_dimensions, df_events_exploded

    def _request_profile_ids(self, df_profile_dimensions: pd.DataFrame) -> list(int):
        grpc_hook = self._get_grpc_hook()
        profile_ids = []
        for i in range(0, len(df_profile_dimensions), self.PROFILESVC_REQ_BATCH_SIZE):
            self.log.info(f"Processing profilesvc_req batch {i} ~ {i + self.PROFILESVC_REQ_BATCH_SIZE}")
            res_gen = self._grpc_request(grpc_hook=grpc_hook, df_profile_dimensions=df_profile_dimensions[i: i + self.PROFILESVC_REQ_BATCH_SIZE])
            for res in res_gen:
                profile_ids.extend(res.profile_ids)
        return profile_ids

    def _grpc_request(self, grpc_hook, df_profile_dimensions: pd.DataFrame) -> list():
        self.log.info("Calling profilesvc")
        stub_class = getattr(grpc_stubs, self.stub_class_func)()
        mapper_func = getattr(grpc_mappers, self.mapper_func)
        data = {'request': mapper_func(df_profile_dimensions)}
        res = grpc_hook.run(stub_class, self.call_func, data=data)
        return res

    def _merge_profiles(self, profile_ids: list(int), df_clicks_exploded: pd.DataFrame) -> pd.DataFrame:
        df_profile_ids = pd.DataFrame(profile_ids, columns=['profile_id'])
        df_profile_joined_events = pd.merge(df_clicks_exploded.reset_index(drop=True), df_profile_ids.reset_index(drop=True), sort=False, copy=False, left_index=True, right_index=True)[self.data_source.get_schema_mapping()]
        self.log.info("joined profiles")
        return df_profile_joined_events

    def _export_to_s3(self, df_profile_joined_events: pd.DataFrame, dest_s3_bucket: str, dest_s3_key: str) -> None:
        # Convert pd.DataFrame to json, since Athena cannot parse parquet as MAP
        df_profile_joined_events.to_json(f'{self.dag_run_uuid}_joined_df_events.json.gz', compression='gzip', orient='records', lines=True)
        s3_hook = self._get_s3_hook()
        with open(f'{self.dag_run_uuid}_joined_df_events.json.gz', 'rb') as obj:
            s3_hook.load_file_obj(file_obj=obj, bucket_name=dest_s3_bucket, key=dest_s3_key + '.gz', replace=True, )
        self.log.info(f"Exported {obj} to {dest_s3_bucket}/{dest_s3_key}")

    @get_dag_run_uuid
    def execute(self, context) -> None:
        for s3_bucket, s3_key, dest_s3_key in self._process_xcom_messages(context=context, source_task=self.message_source_task_id):
            df_events = self._read_events_from_s3(src_s3_bucket=s3_bucket, src_s3_key=s3_key)
            for i in range(0, len(df_events), self.DATA_MERGE_BATCH_SIZE):
                self.log.info(f"Processing profile merge batch {i} ~ {i + self.DATA_MERGE_BATCH_SIZE}")
                df_profile_dimensions, df_events_exploded = self._extract_profile_info(df_events=df_events[i: i + self.DATA_MERGE_BATCH_SIZE], )
                profile_ids = self._request_profile_ids(df_profile_dimensions=df_profile_dimensions)
                df_profile_joined_events = self._merge_profiles(profile_ids=profile_ids, df_clicks_exploded=df_events_exploded)
                self._export_to_s3(df_profile_joined_events=df_profile_joined_events, dest_s3_bucket=s3_bucket, dest_s3_key=os.path.join(os.path.split(dest_s3_key)[0], str(i) + '_' + os.path.split(dest_s3_key)[1]))
