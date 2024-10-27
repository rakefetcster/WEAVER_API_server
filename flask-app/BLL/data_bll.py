from DAL.mongo_connectin import MongoDal
from pyspark.sql import functions as F
import json

class DataBL:
    def __init__(self):
        self.__connection_mongo = MongoDal()
        self.user_df=self.__connection_mongo.spark_user_df()
        self.order_df=self.__connection_mongo.spark_order_df()
        self.logs=self.__connection_mongo.spark_logs_df()

    def create_new_dict(self,data):
        order_list = []
        for row in data:
            new_dict = {}
            row_dict= json.loads(row)
            new_dict.update(row_dict)
            order_list.append(new_dict)
        return order_list
    
    def get_orders(self):
        joined_df = self.user_df.join(self.order_df, self.user_df.userId == self.order_df.userId, "inner")
        orders_per_user = joined_df.groupBy(self.user_df.userId) \
        .agg(
                F.count(self.order_df.orderId).alias("order_count"),    # Count the number of orders per user
                F.collect_list(self.order_df.orderId).alias("order_ids")  # Collect the list of orderIds per user
        )
        # orders_per_user.show(truncate=False,n=50)
        # print( orders_per_user.toJSON().collect() )
        results = orders_per_user.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data
        
    def get_enters(self):
        joined_df = self.user_df.join(self.logs, self.user_df.userId == self.logs.userId, "inner")
        # Group by 'userId', count distinct 'landing_url', and collect the list of landing URLs
        pages_per_user = joined_df.groupBy(self.user_df.userId) \
            .agg(
                F.countDistinct(self.logs.landing_url).alias("page_count"),  # Count distinct pages visited per user
                F.collect_list(self.logs.landing_url).alias("pages_visited")  # Collect list of landing URLs per user
            )

        # Show the result (userId, page_count, and list of landing URLs)
        # pages_per_user.show(truncate=False)
        results = pages_per_user.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data

               
    