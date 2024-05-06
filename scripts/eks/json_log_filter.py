import click
import logging
import json

from pyspark.sql.types import BooleanType
from pyspark.sql.functions import col, date_format, from_json, lit, to_timestamp, udf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JSONLogFilter").getOrCreate()
# Don't write _SUCCESS files at the output location
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

logger = logging.getLogger('py4j')


def is_json(log: str):
    try:
        json.loads(log)
    except ValueError:
        return False
    return True


@click.command()
@click.option("--input_file_location", required=True)
def main(input_file_location: str, ):
    is_json_udf = udf(is_json, BooleanType())

    logger.info(f"Reading df from {input_file_location}")
    df = spark.read.json(input_file_location)

    # Fill log_type as "general" as a default
    json_filtered = df.filter(df.log.isNotNull() & is_json_udf(df.log)).select("log", "time", "kubernetes.namespace_name")
    json_schema = spark.read.json(json_filtered.rdd.map(lambda row: row.log)).schema
    json_renamed = json_filtered.withColumn('log', from_json(col('log'), json_schema)).select(
        col('namespace_name').alias('service'),
        date_format(to_timestamp(col('time')), "yyyy").alias('year'),
        date_format(to_timestamp(col('time')), "MM").alias('month'),
        date_format(to_timestamp(col('time')), "dd").alias('day'),
        date_format(to_timestamp(col('time')), "HH").alias('hour'),
        to_timestamp(col('time')).alias('log_timestamp'),
        col('log.*')
    )

    if 'log_type' not in json_renamed.columns:
        json_renamed = json_renamed.withColumn("log_type", lit("general"))

    if len(json_renamed.head(1)) > 0:
        json_renamed.coalesce(1).write.partitionBy("service", "year", "month", "day", "hour", "log_type",).mode("overwrite").option("compression", "gzip").json('s3://prod-buzzvil-data-lake/eks/cluster-json-logs/')


if __name__ == "__main__":
    main()
