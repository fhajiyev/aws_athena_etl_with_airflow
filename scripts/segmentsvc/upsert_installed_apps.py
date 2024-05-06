import click
import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format


spark = SparkSession \
    .builder \
    .appName("UpdateSegmentsvcTagInstalledApps") \
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
    logger.info(f"SELECT * FROM prod_segmentsvc.g_tag WHERE tag_key = 'installed_app' AND partition_timestamp=TIMESTAMP'{execution_timestamp}'")
    df_g = spark.sql(f"SELECT * FROM prod_segmentsvc.g_tag WHERE tag_key = 'installed_app' AND partition_timestamp=TIMESTAMP'{execution_timestamp}'")

    logger.info(f"SELECT * FROM prod_segmentsvc.g_dim_tag WHERE tag_key = 'installed_app' AND partition_timestamp=TIMESTAMP'{execution_timestamp}'")
    df_g_dim = spark.sql(f"SELECT * FROM prod_segmentsvc.g_dim_tag WHERE tag_key = 'installed_app' AND partition_timestamp=TIMESTAMP'{execution_timestamp}'")

    # Exclude rows from dimension where profile_id exists in the incoming rows
    df_g_dim_excluded = df_g_dim.join(df_g, on=['profile_id'], how='left_anti')

    df_upsert = df_g_dim_excluded.union(df_g)
    df_upsert = df_upsert.withColumn("partition_timestamp", lit(next_execution_timestamp))
    df_upsert = df_upsert.withColumn('update_timestamp', date_format(df_upsert['update_timestamp'], 'yyyy-MM-dd HH:mm:ss'))

    # df_upsert.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").option("path", "s3://prod-buzzvil-data-lake/segmentsvc/gold/dim_tag/installed_apps").mode("append").insertInto("prod_segmentsvc.g_dim_tag")
    year = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%Y')
    month = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%m')
    day = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%d')
    hour = datetime.datetime.strptime(next_execution_timestamp, '%Y-%m-%d %H:%M:%S').strftime('%H')
    df_upsert.write.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode("append").json(path=f"s3://prod-buzzvil-data-lake/segmentsvc/gold/dim_tag/year={year}/month={month}/day={day}/hour={hour}/installed_apps")
    spark.sql(f"ALTER TABLE prod_segmentsvc.g_dim_tag ADD IF NOT EXISTS PARTITION (partition_timestamp='{next_execution_timestamp}') LOCATION 's3://prod-buzzvil-data-lake/segmentsvc/gold/dim_tag/year={year}/month={month}/day={day}/hour={hour}/installed_apps'")


if __name__ == "__main__":
    main()
