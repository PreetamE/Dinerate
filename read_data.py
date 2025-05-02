from pyspark.sql import SparkSession
import config

def load_all_data(spark):
    users_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "").csv(config.USER_PATH)
    reviews_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "").csv(config.REVIEWS_PATH)
    restaurants_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue", "").csv(config.RESTAURANTS_PATH)
    return users_df, reviews_df, restaurants_df