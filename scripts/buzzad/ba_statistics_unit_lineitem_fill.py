import click
import datetime
import logging

from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("UpdateSegmentsvcPropertyDevice") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
    .enableHiveSupport() \
    .getOrCreate()

logger = logging.getLogger('py4j')


@click.command()
@click.option("--execution_timestamp", required=True)
@click.option("--next_execution_timestamp", required=True)
def main(execution_timestamp: str, next_execution_timestamp: str, ):
    year = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y')
    month = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%m')
    day = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%d')
    hour = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%H')

    df_insert = spark.sql(f"""
        SELECT
            DATE_TRUNC('hour', created_at) as data_at,
            unit_id,
            unit_type,
            item_type,
            revenue_type,
            lineitem_id,
            count(*) as fill_count
        FROM
            prod_buzzad.g_fill
        WHERE
            filled = TRUE AND
            partition_timestamp >= TIMESTAMP'{execution_timestamp}' AND
            partition_timestamp < TIMESTAMP'{next_execution_timestamp}'
        GROUP BY
            1,2,3,4,5,6
    """)

    df_insert.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode("overwrite").parquet(path=f"s3://prod-buzzvil-data-lake/buzzad/mart/fill_statistics/year={year}/month={month}/day={day}/hour={hour}/")

    spark.sql(f"""
        ALTER TABLE prod_buzzad.m_fill_statistics
        ADD IF NOT EXISTS PARTITION (partition_timestamp='{next_execution_timestamp}')
        LOCATION 's3://prod-buzzvil-data-lake/buzzad/mart/fill_statistics/year={year}/month={month}/day={day}/hour={hour}/'
    """)


if __name__ == "__main__":
    main()
