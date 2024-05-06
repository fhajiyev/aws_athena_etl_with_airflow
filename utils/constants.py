from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from collections import namedtuple

XcomParam = namedtuple('XcomParam', 'xcom_source_task_id xcom_param_key')

CELERY_CONFIG = DEFAULT_CELERY_CONFIG
CELERY_CONFIG['worker_max_tasks_per_child'] = 10

REDSHIFT_UNLOAD_OPTIONS = dict({
    'uncover_csv': [
        'HEADER',
        'FORMAT AS CSV',
        'ALLOWOVERWRITE',
        'NULL AS \'\'',
        'REGION AS \'ap-northeast-1\'',
        'PARALLEL OFF'
    ],
    'default_csv': [
        'DELIMITER AS \'|\'',
        'ALLOWOVERWRITE',
        'ESCAPE',
        'ADDQUOTES',
        'NULL AS \'\'',
    ],
    'ml_sagemaker_csv': [
        'DELIMITER AS \',\'',
        'ALLOWOVERWRITE',
        'HEADER',
        'ESCAPE',
        'ADDQUOTES',
        'NULL AS \'\'',
        'MAXFILESIZE 100 MB',
        'REGION AS \'ap-northeast-1\''
    ],
    'default_parquet': [
        'FORMAT PARQUET'
    ]
})

REDSHIFT_COPY_OPTIONS = dict({
    'athena_csv': [
        'DELIMITER AS \',\'',
        'IGNOREHEADER 1',
        'TIMEFORMAT \'auto\'',
        'ESCAPE',
        'REMOVEQUOTES',
        'EMPTYASNULL',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
    ],
    'default_csv': [
        'DELIMITER AS \'|\'',
        'IGNOREHEADER 1',
        'TIMEFORMAT \'auto\'',
        'ESCAPE',
        'REMOVEQUOTES',
        'EMPTYASNULL',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
    ],
    'csv_max_error_1000': [
        'DELIMITER AS \'|\'',
        'IGNOREHEADER 1',
        'TIMEFORMAT \'auto\'',
        'ESCAPE',
        'REMOVEQUOTES',
        'EMPTYASNULL',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
        'MAXERROR 1000'
    ],
    'adjust_csv': [
        'DELIMITER AS \',\'',
        'IGNOREHEADER 1',
        'TIMEFORMAT \'epochsecs\'',
        'CSV',
        'QUOTE AS \'"\'',
        'EMPTYASNULL',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
        'GZIP',
        'TRUNCATECOLUMNS'
    ],
    'default_json': [
        'GZIP',
        'JSON \'auto\'',
        'TIMEFORMAT \'auto\'',
        'ACCEPTINVCHARS',
        'TRUNCATECOLUMNS',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
    ],
    'jsonpath': [
        'GZIP',
        'JSON \'{jsonpath_location}\'',
        'TIMEFORMAT \'{timeformat}\'',
        'ACCEPTINVCHARS',
        'TRUNCATECOLUMNS',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
    ],
    'jsonpath_error_max_100': [
        'GZIP',
        'JSON \'{jsonpath_location}\'',
        'TIMEFORMAT \'{timeformat}\'',
        'ACCEPTINVCHARS',
        'TRUNCATECOLUMNS',
        'STATUPDATE OFF',
        'COMPUPDATE OFF',
        'MAXERROR 100'
    ]
})

DEFAULT_MYSQL_BATCH_PERIOD = 'hour'
DEFAULT_MYSQL_BATCH_SIZE = 10000
MAX_MYSQL_BATCH_SIZE = 1000000000000000
DEFAULT_MIGRATION_VERSION = '0__init__19700101_0000'
