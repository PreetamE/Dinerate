from pyspark.sql.functions import *

def clean_restaurants(restaurants_df):
    restaurantdf = restaurants_df.drop("online_delivery").drop("zipcode").drop("city")
    restdf = restaurantdf.na.drop(subset=["restaurant_name"])

    rest_df_with_slug = restdf.withColumn(
        "slug",
        regexp_replace(
            regexp_replace(
                regexp_replace(trim(lower(col("restaurant_name"))), r"\\s*and\\s*", " "),
                r"[,\\s]+", "-"
            ),
            r"-+", "-"
        )
    )

    rest_df_with_category = rest_df_with_slug.withColumn(
        "rating_category",
        when(col("ratings") > 4, "Best")
        .when((col("ratings") >= 3) & (col("ratings") <= 4), "Better")
        .when((col("ratings") >= 2) & (col("ratings") < 3), "Good")
        .otherwise("Not Recommended")
    ).withColumnRenamed("ratings", "restaurant_ratings")

    rest_df_with_comma_count = rest_df_with_category.withColumn(
        "comma_count",
        length(col("address")) - length(regexp_replace(col("address"), ",", ""))
    )

    rest_df_with_parts = rest_df_with_comma_count.withColumn("address_parts", split(col("address"), ","))

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

    rest_df_cleaned = rest_df_split.drop("address_parts", "comma_count", "address")
    return rest_df_cleaned
