import config
import psycopg2

def write_to_postgres(df):
    def insert_partition(partition):
        connection = psycopg2.connect(
            host="localhost",
            database="dinerate",
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD
        )
        cursor = connection.cursor()

        for row in partition:
            cursor.execute("""
                INSERT INTO dinerate_full_data (
                     restaurant_id,customer_id,review_id,comments,customer_ratings,customer_name,email,physically_handicapped,restaurant_name,cuisine,restaurant_ratings,wheel_chair_accessible,restaurant_timings,slug,rating_category,street,city,state
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()

    df.foreachPartition(insert_partition)
