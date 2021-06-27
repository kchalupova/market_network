import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import os


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


schema = 'id STRING, conversation_id STRING, created_at STRING, date DATE, time STRING, timezone STRING, user_id STRING, username STRING, name STRING, place STRING, tweet STRING, language STRING, mentions STRING,urls STRING,photos STRING,replies_count INT,retweets_count INT,likes_count INT,hashtags STRING,cashtags STRING,link STRING,retweet STRING,quote_url STRING,video STRING,thumbnail STRING,near STRING,geo STRING,source STRING,user_rt_id STRING,user_rt STRING,retweet_id STRING,reply_to STRING,retweet_date STRING,translate STRING,trans_src STRING,trans_dest STRING'


files = os.listdir("Ticker_tweets")

for f in files:
    df = (
        spark.read.format("csv").option("header", "true").option("multiline", "true").load(f"Ticker_tweets/{f}", schema=schema)
        .select('id', 'created_at', 'username', 'tweet', 'replies_count', 'retweets_count', 'likes_count')
        .withColumnRenamed("username", "writer")
        .withColumnRenamed("likes_count", "like_num")
        .withColumnRenamed("replies_count", "comment_num")
        .withColumnRenamed("retweets_count", "retweet_num")
    )


    df = df.withColumn("company_id", F.lit(f[:-4]))
    
    (
        df
        .coalesce(1)
        .write
        .format('csv')
        .option("header", True)
        .option("sep", ",")
        .mode("overwrite")
        .save(f"Ticker_tweets_company/{f}")
    )



df = spark.read.format("csv").option("header", "true").option("multiline", "true").load("Ticker_tweets_company/*.csv")

(
    df
    .coalesce(1)
    .write
    .format('csv')
    .option("header", True)
    .option("sep", "\t")
    .mode("overwrite")
    .save("merged.csv")
)
