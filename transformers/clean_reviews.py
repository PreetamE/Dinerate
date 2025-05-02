from pyspark.sql.functions import lit, lower, col, when, create_map

def clean_reviews(reviews_df):
    rating_map = {
        "bad": 1, "two": 2, "three": 3, "four": 4, "five": 5, "excellent": 5
    }
    rating_map_expr = create_map(*[
        item for kv in rating_map.items() for item in (lit(kv[0]), lit(kv[1]))
    ])

    return reviews_df.withColumn(
        "ratings_clean",
        when(col("ratings").cast("int").isNotNull(), col("ratings").cast("int"))
        .otherwise(rating_map_expr[lower(col("ratings"))])
    ).drop("ratings").withColumnRenamed("ratings_clean", "customer_ratings")
