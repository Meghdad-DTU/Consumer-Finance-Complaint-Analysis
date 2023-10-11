from dotenv import dotenv_values
from pyspark.sql import SparkSession


aws_credentials = dotenv_values(".env")
access_key_id = aws_credentials['AWS_ACCESS_KEY_ID']
secret_access_key= aws_credentials['AWS_SECRET_ACCESS_KEY']


spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memoryOverhead", "8g") \
    .config('spark.jars.packages',"com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3")\
    .getOrCreate()

spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", access_key_id)
spark_session._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", secret_access_key)