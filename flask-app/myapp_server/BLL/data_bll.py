from DAL.mongo_connectin import MongoDal
from pyspark.sql import functions as F

class DataBL:
    def __init__(self):
        self.__connection_mongo = MongoDal()
        self.user_df=self.__connection_mongo.spark_user_df()
        self.order_df=self.__connection_mongo.spark_order_df()
        self.logs=self.__connection_mongo.spark_logs_df()

    def get_orders(self):
        joined_df = self.user_df.join(self.order_df, self.user_df.userId == self.order_df.userId, "inner")
        orders_per_user = joined_df.groupBy(self.user_df.userId) \
        .agg(
                F.count(self.order_df.orderId).alias("order_count"),    # Count the number of orders per user
                F.collect_list(self.order_df.orderId).alias("order_ids")  # Collect the list of orderIds per user
        )
        orders_per_user.show(truncate=False,n=50)
        return orders_per_user

    def get_enters(self):
        joined_df = self.user_df.join(self.logs, self.user_df.userId == self.logs.userId, "inner")
        # Group by 'userId', count distinct 'landing_url', and collect the list of landing URLs
        pages_per_user = joined_df.groupBy(self.user_df.userId) \
            .agg(
                F.countDistinct(self.logs.landing_url).alias("page_count"),  # Count distinct pages visited per user
                F.collect_list(self.logs.landing_url).alias("pages_visited")  # Collect list of landing URLs per user
            )

        # Show the result (userId, page_count, and list of landing URLs)
        pages_per_user.show(truncate=False)

               
    