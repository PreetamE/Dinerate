def join_all(users_df, reviews_df, restaurants_df):
    reviews_with_users = reviews_df.join(users_df, on="customer_id", how="left")
    full_df = reviews_with_users.join(restaurants_df, on="restaurant_id", how="left")
    return full_df
