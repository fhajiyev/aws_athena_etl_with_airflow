import enum


class RedshiftLoadType(enum.Enum):
    REPLACE = 'replace'
    INCREMENTAL = 'incremental'
    UPSERT = 'upsert'
    DEDUPLICATED_UPSERT = 'deduplicated_upsert'
