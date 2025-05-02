from pyspark.sql import SparkSession
from read_data import load_all_data
from transformers.clean_users import clean_users
from transformers.clean_reviews import clean_reviews
from transformers.clean_restaurants import clean_restaurants
from join_all import join_all
from write import write_to_postgres
import config

spark = SparkSession.builder \
    .appName("DineRate Project") \
    .config("spark.jars", config.JDBC_DRIVER_PATH) \
    .getOrCreate()

users_df, reviews_df, restaurants_df = load_all_data(spark)

cleaned_users = clean_users(users_df)
cleaned_reviews = clean_reviews(reviews_df)
cleaned_restaurants = clean_restaurants(restaurants_df)

final_df = join_all(cleaned_users, cleaned_reviews, cleaned_restaurants)

# select matching columns
final_df = final_df.select(
    "restaurant_id",
    "customer_id",
    "review_id",
    "comments",
    "customer_ratings",
    "customer_name",
    "email",
    "physically_handicapped",
    "restaurant_name",
    "cuisine",
    "restaurant_ratings",
    "wheel_chair_accessible",
    "restaurant_timings",
    "slug",
    "rating_category",
    "street",
    "city",
    "state"
)

write_to_postgres(final_df)
