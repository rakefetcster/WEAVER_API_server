from pyspark.sql import SparkSession


class MongoDal:
    def __init__(self):
        self.conn_uri = "mongodb+srv://weaver-api:KbWZdxpj5DunE3St@weaver.sdy1p.mongodb.net/quantumlevitation?retryWrites=true&w=majority&appName=Weaver"
        self.my_spark = SparkSession \
            .builder \
            .appName("myApp") \
            .config("spark.executor.memory", "11g") \
            .config("spark.mongodb.read.connection.uri", self.conn_uri) \
            .config("spark.mongodb.write.connection.uri", self.conn_uri) \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.3") \
            .getOrCreate()
       

    
    def spark_order_df(self):
        orders_df = (self.my_spark.read
            .format("mongodb")
            .option("database", "quantumlevitation")
            .option("collection", "orders")
            .option("sampleSize", 10) # this size is used to determine schema
            .option('uri', self.conn_uri)
            .load())
        # orders_df.show()
        return orders_df
    
    def spark_user_df(self):
        users_df = (self.my_spark.read
            .format("mongodb")
            .option("database", "quantumlevitation")
            .option("collection", "users")
            .option("sampleSize", 10) # this size is used to determine schema
            .option('uri', self.conn_uri)
            .load())
        # orders_df.show()
        return users_df
    
    def spark_logs_df(self):
        logs_df = (self.my_spark.read
            .format("mongodb")
            .option("database", "quantumlevitation")
            .option("collection", "logs")
            .option("sampleSize", 10) # this size is used to determine schema
            .option('uri', self.conn_uri)
            .load())
        return logs_df