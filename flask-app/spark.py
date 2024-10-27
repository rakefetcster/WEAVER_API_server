from pyspark.sql import SparkSession
from pyspark.sql import functions as F

conn_uri = "mongodb+srv://weaver-api:KbWZdxpj5DunE3St@weaver.sdy1p.mongodb.net/quantumlevitation?retryWrites=true&w=majority&appName=Weaver"

# my_spark = SparkSession \
#     .builder \
#     .appName("myApp") \
#     .getOrCreate()

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.mongodb.read.connection.uri", conn_uri) \
    .config("spark.mongodb.write.connection.uri", conn_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3") \
    .getOrCreate()

# my_spark.sparkContext.setLogLevel("DEBUG")

users_df = (my_spark.read
      .format("mongodb")
      .option("database", "quantumlevitation")
      .option("collection", "users")
      .option("sampleSize", 10) # this size is used to determine schema
      .option('uri', conn_uri)
      .load())
# users_df.show()

orders_df = (my_spark.read
      .format("mongodb")
      .option("database", "quantumlevitation")
      .option("collection", "orders")
      .option("sampleSize", 10) # this size is used to determine schema
      .option('uri', conn_uri)
      .load())
# orders_df.show()

logs_df = (my_spark.read
      .format("mongodb")
      .option("database", "quantumlevitation")
      .option("collection", "logs")
      .option("sampleSize", 10) # this size is used to determine schema
      .option('uri', conn_uri)
      .load())


joined_df = users_df.join(orders_df, users_df.userId == orders_df.userId, "inner")

orders_per_user = joined_df.groupBy(users_df.userId) \
    .agg(
        F.count(orders_df.orderId).alias("order_count"),    # Count the number of orders per user
        F.collect_list(orders_df.orderId).alias("order_ids")  # Collect the list of orderIds per user
    )

orders_per_user.show(truncate=False,n=50)


# Join the two DataFrames on 'userId'
joined_df = users_df.join(logs_df, users_df.userId == logs_df.userId, "inner")

# Group by 'userId', count distinct 'landing_url', and collect the list of landing URLs
pages_per_user = joined_df.groupBy(users_df.userId) \
    .agg(
        F.countDistinct(logs_df.landing_url).alias("page_count"),  # Count distinct pages visited per user
        F.collect_list(logs_df.landing_url).alias("pages_visited")  # Collect list of landing URLs per user
    )

# Show the result (userId, page_count, and list of landing URLs)
pages_per_user.show(truncate=False,n=50)



my_spark.stop()