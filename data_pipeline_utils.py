# data_pipeline_utils.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, split, explode
from pyspark.sql.types import LongType, BooleanType, IntegerType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

def load_csv_json_from_s3(bucket_name: str, spark: SparkSession):
    # Set Hadoop configs for S3 access
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "aws_access_key_id")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "aws_secret_access_key")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    # Load CSV data
    # Define S3 path
    csv_s3_path = "s3a://youtube-trending-data-bucket/raw/csv/"
    # Read CSV file into DataFrame
    csv_data_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_s3_path)
    #reading json
    json_s3_path="s3a://youtube-trending-data-bucket/raw/json/"
    json_data= spark.read.option("multiLine", True).json(json_s3_path)

    csv_data_df.show()

    json_data_exploded = json_data.select(explode(col("items")).alias("item"))
    json_data_df = json_data_exploded.select(
        col("item.id").alias("id"),
        col("item.snippet.channelId").alias("channelId"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.assignable").alias("assignable")
    ) 
    print(csv_data_df, json_data_df)
    #return csv_data_df, json_data_df




def transform_csv_json_data(csv_data_df, json_data_df):

        columns_to_cast = {
            'category_id': LongType(),
            'views': LongType(),
            'likes': LongType(),
            'dislikes': LongType(),
            'comment_count': LongType(),
            'comments_disabled': BooleanType(),
            'ratings_disabled': BooleanType(),
            'video_error_or_removed': BooleanType()
        }
        for column, data_type in columns_to_cast.items():
            csv_data_df = csv_data_df.withColumn(column, col(column).cast(data_type))

        csv_data_df = csv_data_df.withColumnRenamed('title', 'video_title')
        csv_data_df = csv_data_df.withColumn('trending_date', to_date(col('trending_date'), 'yy.dd.MM'))
        csv_data_df = csv_data_df.withColumn('publish_time', to_timestamp(col('publish_time')))
        #schema modification for json files
        json_data_df = json_data_df.withColumn('id', col('id').cast(IntegerType()))
        json_data_df = json_data_df.withColumnRenamed('id', 'category_id') #renaming the column name same as that in csv files

        complete_df=csv_data_df.join(json_data_df, on ='category_id', how="inner")
        return complete_df


# Write final data to S3 partitioned by region
def write_processed_data_to_s3(df, bucket_name: str):
    df.write.partitionBy("region") \
        .mode("overwrite") \
        .parquet(f"s3a://{bucket_name}/processed/parquet_output/")


def load_parquet_to_redshift(spark: SparkSession, s3_parquet_path: str, redshift_table: str,
                              jdbc_url: str, redshift_user: str, redshift_password: str, temp_s3_dir: str):

        df = spark.read.parquet(s3_parquet_path)

        df.write \
            .format("com.databricks.spark.redshift") \
            .option("url", jdbc_url) \
            .option("dbtable", redshift_table) \
            .option("user", redshift_user) \
            .option("password", redshift_password) \
            .option("tempdir", temp_s3_dir) \
            .mode("append") \
            .save()
