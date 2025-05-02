from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Restaurant Data").getOrCreate()

users_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue","").csv("/Users/preetamepari/Downloads/dinerate_users.csv")

reviews_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue","").csv("/Users/preetamepari/Downloads/dinerate_reviews.csv")

restaurants_df = spark.read.option("header", True).option("inferSchema", True).option("nullValue","").csv("/Users/preetamepari/Downloads/dinerate_restaurants-2.csv")

#dropping signup_date column
userdf = users_df.drop("signup_date")

userdf.show(20)

#dropping online delivery column
restaurantdf = restaurants_df.drop("online_delivery").drop("zipcode").drop("city")
restaurantdf.show(30)

#dropping the records where name is missing
restdf = restaurantdf.na.drop(subset=["restaurant_name"])
restdf.show(30)
rest_df_with_slug = restdf.withColumn(
    "slug",
    regexp_replace(
        regexp_replace(
            regexp_replace(trim(lower(col("restaurant_name"))), r"\s*and\s*", " "), # remove 'and' with surrounding spaces
            r"[,\s]+", "-" # replace commas and spaces with hyphens
        ),
        r"-+", "-" # ensure no multiple hyphens appear
    )
)

rest_df_with_slug.show(30)

rest_df_with_category = rest_df_with_slug.withColumn(
    "rating_category",
    when(col("ratings") > 4, "Best")
    .when((col("ratings") >= 3) & (col("ratings") <= 4), "Better")
    .when((col("ratings") >= 2) & (col("ratings") < 3), "Good")
    .otherwise("Not Recommended")
)

rest_df_with_category.show(20)
from pyspark.sql.functions import split

# Split address into array using comma
rest_df_split = rest_df_with_category.withColumn("address_parts", split(col("address"), ","))

# Extract components
rest_df_split = rest_df_split.withColumn("street", col("address_parts").getItem(0)) \
    .withColumn("city", col("address_parts").getItem(1)) \
    .withColumn("state", col("address_parts").getItem(2))

# Optional: drop the intermediate array column
rest_df_split = rest_df_split.drop("address").drop("address_parts")

rest_df_split.select("restaurant_name", "street", "city", "state").show(10, truncate=False)

rest_df_split.show(20)

######################

# Step 1: Count number of commas in address
rest_df_with_comma_count = rest_df_with_category.withColumn(
    "comma_count",
    length(col("address")) - length(regexp_replace(col("address"), ",", ""))
)

# Step 2: Split into parts
rest_df_with_parts = rest_df_with_comma_count.withColumn("address_parts", split(col("address"), ","))

# Step 3: Assign values conditionally
rest_df_split = rest_df_with_parts.withColumn(
    "street",
    when(col("comma_count") >= 2, trim(col("address_parts").getItem(0))).otherwise(lit(None))
).withColumn(
    "city",
    when(col("comma_count") >= 2, trim(col("address_parts").getItem(1))).otherwise(lit(None))
).withColumn(
    "state",
    when(col("comma_count") >= 2, trim(col("address_parts").getItem(2))).otherwise(lit(None))
)

# Step 4: Drop intermediate columns if desired
rest_df_cleaned = rest_df_split.drop("address_parts", "comma_count", "address")

# Step 5: Display the final result
rest_df_cleaned.select("restaurant_name", "street", "city", "state").show(30, truncate=False)

rating_map = {
    "bad": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "excellent": 5
}

# Define your extended ratings mapping
rating_map = {
    "bad": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "excellent": 5
}

# Create a map expression
rating_map_expr = create_map(*[
    item for kv in rating_map.items() for item in (lit(kv[0]), lit(kv[1]))
])

# Apply the cleaned transformation using bracket syntax
reviews_df_transformed = reviews_df.withColumn(
    "ratings_clean",
    when(col("ratings").cast("int").isNotNull(), col("ratings").cast("int"))
    .otherwise(rating_map_expr[lower(col("ratings"))])
)
reviews_df_transformed = reviews_df_transformed.drop("ratings") \
    .withColumnRenamed("ratings_clean", "ratings")

reviews_df_transformed.show(20)
rest_df_cleaned.show(20)
userdf.show(20)

reviews_with_users = reviews_df_transformed.join(
    userdf, on="customer_id", how="left"
)

# Step 2: Join the result with restaurants
full_joined_df = reviews_with_users.join(
    rest_df_cleaned, on="restaurant_id", how="left"
)

full_joined_df.show(30)