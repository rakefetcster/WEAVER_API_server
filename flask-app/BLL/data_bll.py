from DAL.mongo_connectin import MongoDal
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, countDistinct, from_utc_timestamp, unix_timestamp, lag, when, sum, first, struct, count, min, max, collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType
from pyspark.sql.window import Window
from datetime import datetime


import json

class DataBL:
    def __init__(self):
        self.__connection_mongo = MongoDal()
        self.user_df=self.__connection_mongo.spark_user_df()
        self.order_df=self.__connection_mongo.spark_order_df()
        self.logs_df=self.__connection_mongo.spark_logs_df()
        self.sources_df = self.__connection_mongo.spark_sources_df()
        self.campaigns_df = self.__connection_mongo.spark_campaigns_df()

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
        joined_df = self.user_df.join(self.logs_df, self.user_df.userId == self.logs_df.userId, "inner")
        # Group by 'userId', count distinct 'landing_url', and collect the list of landing URLs
        pages_per_user = joined_df.groupBy(self.user_df.userId) \
            .agg(
                F.countDistinct(self.logs_df.landing_url).alias("page_count"),  # Count distinct pages visited per user
                F.collect_list(self.logs_df.landing_url).alias("pages_visited")  # Collect list of landing URLs per user
            )

        # Show the result (userId, page_count, and list of landing URLs)
        # pages_per_user.show(truncate=False)
        results = pages_per_user.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data
    

        
    def get_visitors_by_session_new(self):
        """
        Get the number of visitors per 30-minute session, counting users only once per session, and showing 
        one result per source and date. Data is filtered to only include the last year from the current date.
        :return: A dictionary of visitors count grouped by date and source_name.
        """
        
        # Step 1: Get the current date and calculate the date range for the last year
        current_date = datetime.today()
        one_year_ago = current_date.replace(year=current_date.year - 1)  # Date one year ago from today
        
        # Use these dates to filter the data
        start_date = one_year_ago.date()
        end_date = current_date.date()

        # Step 2: Extract the date part from the timestamp (no conversion to Israel Standard Time)
        logs_df_ist = self.logs_df.withColumn("date", F.to_date(F.col("timestamp")))

        # Step 3: Filter data between the start and end dates (last year)
        logs_df_filtered_last_year = logs_df_ist.filter((F.col("date") >= F.lit(start_date)) & (F.col("date") <= F.lit(end_date)))

        # Step 4: Join the logs with the sources data on 'source_id'
        logs_sources_df = logs_df_filtered_last_year.join(self.sources_df, logs_df_filtered_last_year.source_id == self.sources_df.source_id, "inner")
        logs_sources_df.groupBy("date", "source_name", "userId").count().show(truncate=False)

        # Step 5: Sort the logs by userId and timestamp to track visits over time
        logs_sources_df = logs_sources_df.orderBy("userId", "timestamp")

        # Step 6: Define the window specification for calculating session start times
        window_spec = Window.partitionBy("userId").orderBy("timestamp")

        # Step 7: Calculate the time difference between consecutive visits
        logs_sources_df = logs_sources_df.withColumn(
            "prev_timestamp", F.lag("timestamp").over(window_spec)
        )

        logs_sources_df = logs_sources_df.withColumn(
            "time_diff", F.when(F.col("prev_timestamp").isNotNull(), F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long")).otherwise(F.lit(0))
        )

        # Step 8: Create a session ID by incrementing whenever the time difference exceeds 30 minutes
        logs_sources_df = logs_sources_df.withColumn(
            "session_id", F.sum(F.when(F.col("time_diff") > 1800, 1).otherwise(0)).over(window_spec)
        )

        # Step 9: Group by date and source_name, and count distinct users (visitors) for each group
        visitor_count_df = logs_sources_df.groupBy("date", "source_name").agg(
            F.countDistinct("userId").alias("visitors")
        )

        # Step 10: Collect the results as JSON or return them in your desired format
        # results = visitor_count_df.toJSON().collect()

        # Step 11: Create a new dictionary from the results (if needed)
        # new_data = self.create_new_dict(results)
        return visitor_count_df
        
    def get_logs(self):
        # Step 1: Get the current date and calculate the date range for the last year
        current_date = datetime.today()
        one_year_ago = current_date.replace(year=current_date.year - 1)

        start_date = one_year_ago.date()
        end_date = current_date.date()

        # Step 2: Check if 'utm_source' exists in the schema, otherwise handle its absence
        columns = self.logs_df.columns
        if 'utm_source' in columns:
            log_df_sorted = self.logs_df.select(
                col("timestamp"),
                col("source_id"),
                col("userId"),
                col("referrer"),
                col("landing_url"),
                F.coalesce(col("utm_source"), F.lit("Unknown")).alias("Campaign name")
            )
        else:
            log_df_sorted = self.logs_df.select(
                col("timestamp"),
                col("source_id"),
                col("userId"),
                col("referrer"),
                col("landing_url"),
                F.lit("Unknown").alias("Campaign name")
            )

        # Step 3: Filter out rows where userId is null
        log_df_sorted = log_df_sorted.filter(col("userId").isNotNull())

        # Step 4: Join with the campaigns collection to get the campaign name
        # Assuming that 'source_id' in logs corresponds to '_id' in campaigns
        campaigns_df = self.campaigns_df.select("uuid", "type", "description", "params")
        
        log_df_sorted = log_df_sorted.join(
            campaigns_df,
            log_df_sorted["source_id"] == campaigns_df["uuid"],  # Adjust this join condition based on actual relationship
            how="left"
        ).select(
            col("timestamp"),
            col("source_id"),
            col("userId"),
            col("referrer"),
            col("landing_url"),
            F.coalesce(campaigns_df["description"], F.lit("Unknown")).alias("Campaign name")
        )

        # Step 5: Sort the DataFrame by userId and timestamp
        log_df_sorted = log_df_sorted.orderBy("userId", "timestamp")

        # Step 6: Create session_id with improved session partitioning logic
        window_spec = Window.partitionBy("userId").orderBy("timestamp")

        # Calculate time difference in seconds between consecutive timestamps
        log_df_sorted = log_df_sorted.withColumn(
            "time_diff", 
            unix_timestamp(col("timestamp")) - unix_timestamp(lag("timestamp", 1).over(window_spec))
        )

        # Create session_id based on time gaps (30 minutes inactivity creates a new session)
        log_df_sorted = log_df_sorted.withColumn(
            "session_break",
            when(col("time_diff") > 1800, 1).otherwise(0).cast(IntegerType())
        )

        # Cumulative sum to create session_id
        log_df_sorted = log_df_sorted.withColumn(
            "session_id", 
            sum("session_break").over(Window.partitionBy("userId").orderBy("timestamp")) + 1
        )

        # Step 7: Prepare full session information with all fields, including timestamp and userId
        full_session_df = log_df_sorted.select(
            "userId", 
            "source_id", 
            "referrer", 
            "landing_url", 
            "Campaign name", 
            "timestamp"
        )

        # Step 8: Group and aggregate session data, including all fields in full_session
        log_df_sessions = full_session_df.groupBy(
            "userId", "source_id", "referrer", "landing_url", "Campaign name"
        ).agg(
            count("timestamp").alias("entries_count"),
            collect_list(
                struct(
                    col("timestamp"),
                    col("source_id"),
                    col("referrer"),
                    col("landing_url"),
                    col("Campaign name"),
                    col("userId")
                )
            ).alias("full_session")
        )

        # Step 9: Construct the final DataFrame with timestamp outside full_session
        final_df = log_df_sessions.select(
            col("userId"),
            col("source_id"),
            col("referrer"),
            col("landing_url"),
            col("Campaign name"),
            col("full_session"),
            # Take the timestamp from the last entry in the full_session for session-level data
            F.col("full_session").getItem(F.size("full_session") - 1).getField("timestamp").alias("timestamp")
        )

        # Collect and convert to the desired JSON structure
        results = final_df.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data

    
    def get_logs1(self):
        # Step 1: Get the current date and calculate the date range for the last year
        current_date = datetime.today()
        one_year_ago = current_date.replace(year=current_date.year - 1)

        start_date = one_year_ago.date()
        end_date = current_date.date()

        # Step 2: Check if 'utm_source' exists in the schema, otherwise handle its absence
        columns = self.logs_df.columns
        if 'utm_source' in columns:
            log_df_sorted = self.logs_df.select(
                col("timestamp"),
                col("source_id"),
                col("userId"),
                col("referrer"),
                col("landing_url"),
                F.coalesce(col("utm_source"), F.lit("Unknown")).alias("Campaign name")
            )
        else:
            log_df_sorted = self.logs_df.select(
                col("timestamp"),
                col("source_id"),
                col("userId"),
                col("referrer"),
                col("landing_url"),
                F.lit("Unknown").alias("Campaign name")
            )

        # Step 3: Filter out rows where userId is null
        log_df_sorted = log_df_sorted.filter(col("userId").isNotNull())

        # Step 4: Sort the DataFrame by userId and timestamp
        log_df_sorted = log_df_sorted.orderBy("userId", "timestamp")

        # Step 5: Create session_id with improved session partitioning logic
        window_spec = Window.partitionBy("userId").orderBy("timestamp")

        # Calculate time difference in seconds between consecutive timestamps
        log_df_sorted = log_df_sorted.withColumn(
            "time_diff", 
            unix_timestamp(col("timestamp")) - unix_timestamp(lag("timestamp", 1).over(window_spec))
        )

        # Create session_id based on time gaps (30 minutes inactivity creates a new session)
        log_df_sorted = log_df_sorted.withColumn(
            "session_break",
            when(col("time_diff") > 1800, 1).otherwise(0).cast(IntegerType())
        )

        # Cumulative sum to create session_id
        log_df_sorted = log_df_sorted.withColumn(
            "session_id", 
            sum("session_break").over(Window.partitionBy("userId").orderBy("timestamp")) + 1
        )

        # Step 6: Prepare full session information with all fields, including timestamp and userId
        full_session_df = log_df_sorted.select(
            "userId", 
            "source_id", 
            "referrer", 
            "landing_url", 
            "Campaign name", 
            "timestamp"
        )

        # Step 7: Group and aggregate session data, including all fields in full_session
        log_df_sessions = full_session_df.groupBy(
            "userId", "source_id", "referrer", "landing_url", "Campaign name"
        ).agg(
            count("timestamp").alias("entries_count"),
            collect_list(
                struct(
                    col("timestamp"),
                    col("source_id"),
                    col("referrer"),
                    col("landing_url"),
                    col("Campaign name"),
                    col("userId")
                )
            ).alias("full_session")
        )

        # Step 8: Construct the final DataFrame with timestamp outside full_session
        final_df = log_df_sessions.select(
            col("userId"),
            col("source_id"),
            col("referrer"),
            col("landing_url"),
            col("Campaign name"),
            col("full_session"),
            # Take the timestamp from the last entry in the full_session for session-level data
            F.col("full_session").getItem(F.size("full_session") - 1).getField("timestamp").alias("timestamp")
        )

        # Collect and convert to the desired JSON structure
        results = final_df.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data
    
    # def get_logs(self):
    #     # Step 1: Get the current date and calculate the date range for the last year
    #     current_date = datetime.today()
    #     one_year_ago = current_date.replace(year=current_date.year - 1)

    #     start_date = one_year_ago.date()
    #     end_date = current_date.date()

    #     # Step 2: Check if 'utm_source' exists in the schema, otherwise handle its absence
    #     columns = self.logs_df.columns
    #     if 'utm_source' in columns:
    #         log_df_sorted = self.logs_df.select(
    #             col("timestamp").alias("log_timestamp"),
    #             col("source_id"),
    #             col("userId"),
    #             col("referrer"),
    #             col("landing_url"),
    #             F.coalesce(col("utm_source"), F.lit("Unknown")).alias("Initial Campaign name")
    #         )
    #     else:
    #         log_df_sorted = self.logs_df.select(
    #             col("timestamp").alias("log_timestamp"),
    #             col("source_id"),
    #             col("userId"),
    #             col("referrer"),
    #             col("landing_url"),
    #             F.lit("Unknown").alias("Initial Campaign name")
    #         )

    #     # Step 3: Filter out rows where userId is null
    #     log_df_sorted = log_df_sorted.filter(col("userId").isNotNull())

    #     # Step 4: Prepare campaigns DataFrame with a boolean match column
    #     campaigns_with_match = self.campaigns_df.withColumn(
    #         "url_match",
    #         F.when(
    #             F.length(F.trim(col("params"))) > 0,
    #             F.array_contains(
    #                 F.transform(
    #                     F.split(F.lower(col("params")), ","),
    #                     lambda x: F.trim(x)
    #                 ),
    #                 F.lit(None)  # Placeholder, will be replaced in the next step
    #             )
    #         ).otherwise(F.lit(False))
    #     )

    #     # Step 5: Broadcast the campaigns DataFrame to improve join performance
    #     campaigns_with_match = campaigns_with_match.withColumn(
    #         "url_match",
    #         F.expr(
    #             "array_contains(transform(split(lower(trim(params)), ','), x -> trim(x)), " +
    #             "lower(landing_url))"
    #         )
    #     )

    #     # Step 6: Left join with campaigns and select the first matching campaign
    #     log_df_sorted = log_df_sorted.join(
    #         campaigns_with_match,
    #         campaigns_with_match["url_match"] == F.lit(True),
    #         "left"
    #     )

    #     # Step 7: Enhance Campaign Name Assignment
    #     log_df_sorted = log_df_sorted.withColumn(
    #         "Campaign name",
    #         F.coalesce(
    #             F.when(
    #                 self.campaigns_df["type"].isNotNull() & self.campaigns_df["description"].isNotNull(),
    #                 F.concat(self.campaigns_df["type"], F.lit(" - "), self.campaigns_df["description"])
    #             ),
    #             col("Initial Campaign name"),
    #             F.lit("Unknown")
    #         )
    #     )

    #     # Rest of the function remains the same as in previous implementations
    #     # Step 8: Select relevant columns
    #     log_df_sorted = log_df_sorted.select(
    #         col("log_timestamp").alias("timestamp"),
    #         "source_id",
    #         "userId",
    #         "referrer",
    #         "landing_url",
    #         "Campaign name"
    #     )

    #     # Step 9: Sort the DataFrame by userId and timestamp
    #     log_df_sorted = log_df_sorted.orderBy("userId", "timestamp")

    #     # Step 10: Create session_id based on time gaps
    #     window_spec = Window.partitionBy("userId").orderBy("timestamp")

    #     # Calculate time difference in seconds between consecutive timestamps
    #     log_df_sorted = log_df_sorted.withColumn(
    #         "time_diff", 
    #         unix_timestamp(col("timestamp")) - unix_timestamp(lag("timestamp", 1).over(window_spec))
    #     )

    #     # Create session_id based on time gaps (30 minutes inactivity creates a new session)
    #     log_df_sorted = log_df_sorted.withColumn(
    #         "session_break",
    #         when(col("time_diff") > 1800, 1).otherwise(0).cast(IntegerType())
    #     )

    #     # Cumulative sum to create session_id
    #     log_df_sorted = log_df_sorted.withColumn(
    #         "session_id", 
    #         sum("session_break").over(Window.partitionBy("userId").orderBy("timestamp")) + 1
    #     )

    #     # Step 11: Prepare full session information with all fields, including timestamp and userId
    #     full_session_df = log_df_sorted.select(
    #         "userId", 
    #         "source_id", 
    #         "referrer", 
    #         "landing_url", 
    #         "Campaign name", 
    #         "timestamp"
    #     )

    #     # Step 12: Group and aggregate session data
    #     log_df_sessions = full_session_df.groupBy(
    #         "userId", "source_id", "referrer", "landing_url", "Campaign name"
    #     ).agg(
    #         count("timestamp").alias("entries_count"),
    #         collect_list(
    #             struct(
    #                 col("timestamp"),
    #                 col("source_id"),
    #                 col("referrer"),
    #                 col("landing_url"),
    #                 col("Campaign name"),
    #                 col("userId")
    #             )
    #         ).alias("full_session")
    #     )

    #     # Step 13: Final DataFrame with the full session and timestamp information
    #     final_df = log_df_sessions.select(
    #         col("userId"),
    #         col("source_id"),
    #         col("referrer"),
    #         col("landing_url"),
    #         col("Campaign name"),
    #         col("full_session"),
    #         # Take the timestamp from the last entry in the full_session
    #         F.col("full_session").getItem(F.size("full_session") - 1).getField("timestamp").alias("timestamp")
    #     )

    #     # Step 14: Collect and convert to the desired JSON structure
    #     results = final_df.toJSON().collect()
    #     new_data = self.create_new_dict(results)
    #     return new_data

    def get_conversions_by_recent_source_and_date(self, start_date, end_date):
        """
        Show the conversions categorized according to the most recent source, filtered by the provided date range, 
        considering the Israel Standard Time (IST).
        :param start_date: The start date (inclusive) to filter the logs.
        :param end_date: The end date (inclusive) to filter the logs.
        :return: A DataFrame showing conversions by most recent source for each date.
        """

        # Step 1: Convert the UTC timestamps to Israel Standard Time (IST) before filtering
        logs_df_ist = self.logs_df.withColumn("logs_timestamp_ist", F.from_utc_timestamp(F.col("timestamp"), "Asia/Jerusalem"))
        orders_df_ist = self.order_df.withColumn("orders_timestamp_ist", F.from_utc_timestamp(F.col("timestamp"), "Asia/Jerusalem"))

        # Step 2: Ensure start_date and end_date are in the right format
        start_date = F.to_timestamp(F.lit(start_date))
        end_date = F.to_timestamp(F.lit(end_date))

        # Step 3: Filter logs and orders by the provided date range in IST
        logs_df_filtered = logs_df_ist.filter((F.col("logs_timestamp_ist").between(start_date, end_date)))
        orders_df_filtered = orders_df_ist.filter((F.col("orders_timestamp_ist").between(start_date, end_date)))

        # Step 4: Rename userId columns to avoid conflict
        logs_df_ist = logs_df_filtered.withColumnRenamed("userId", "logs_userId")
        orders_df_ist = orders_df_filtered.withColumnRenamed("userId", "orders_userId")

        # Step 5: Join logs with orders to get conversions
        logs_orders_df = logs_df_ist.join(orders_df_ist, logs_df_ist.logs_userId == orders_df_ist.orders_userId, "left")

        # Step 6: Join with sources to get the source details (source_name)
        logs_sources_df = logs_orders_df.join(self.sources_df, logs_orders_df.source_id == self.sources_df.source_id, "left")

        # Step 7: Sort the logs by userId and timestamp to get the most recent source for each user
        logs_sources_df = logs_sources_df.orderBy("logs_userId", "logs_timestamp_ist", ascending=False)

        # Step 8: Define a window spec to get the most recent source per user (first row per user)
        window_spec = Window.partitionBy("logs_userId").orderBy(F.col("logs_timestamp_ist").desc())

        # Step 9: Get the most recent source for each user
        most_recent_sources_df = logs_sources_df.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1).drop("rank")

        # Step 10: Filter rows where an order (conversion) exists
        conversions_df = most_recent_sources_df.filter(F.col("orderId").isNotNull())

        # Step 11: Format the timestamp to include both date and time
        conversions_df = conversions_df.withColumn("date_time", F.date_format(F.col("logs_timestamp_ist"), "yyyy-MM-dd HH:mm:ss"))

        # Step 12: Group by date and most recent source, and count the conversions (orders)
        conversions_count_df = conversions_df.groupBy("date_time", "source_name").agg(
            F.count("orderId").alias("conversion_count")
        )


        results = conversions_count_df.toJSON().collect()
        new_data = self.create_new_dict(results)
        return new_data

    def get_conversions_by_recent_source_and_date_new(self, start_date, end_date):
        """
        Show the conversions categorized according to the most recent source, filtered by the provided date range, 
        considering the Israel Standard Time (IST).
        :param start_date: The start date (inclusive) to filter the logs.
        :param end_date: The end date (inclusive) to filter the logs.
        :return: A DataFrame showing conversions by most recent source for each date.
        """

        # Step 1: Convert the UTC timestamps to Israel Standard Time (IST) before filtering
        logs_df_ist = self.logs_df.withColumn("logs_timestamp_ist", F.from_utc_timestamp(F.col("timestamp"), "Asia/Jerusalem"))
        orders_df_ist = self.order_df.withColumn("orders_timestamp_ist", F.from_utc_timestamp(F.col("timestamp"), "Asia/Jerusalem"))

        # Step 2: Ensure start_date and end_date are in the right format
        start_date = F.to_timestamp(F.lit(start_date))
        end_date = F.to_timestamp(F.lit(end_date))

        # Step 3: Filter logs and orders by the provided date range in IST
        logs_df_filtered = logs_df_ist.filter((F.col("logs_timestamp_ist").between(start_date, end_date)))
        orders_df_filtered = orders_df_ist.filter((F.col("orders_timestamp_ist").between(start_date, end_date)))

        # Step 4: Rename userId columns to avoid conflict
        logs_df_ist = logs_df_filtered.withColumnRenamed("userId", "logs_userId")
        orders_df_ist = orders_df_filtered.withColumnRenamed("userId", "orders_userId")

        # Step 5: Join logs with orders to get conversions
        logs_orders_df = logs_df_ist.join(orders_df_ist, logs_df_ist.logs_userId == orders_df_ist.orders_userId, "left")

        # Step 6: Join with sources to get the source details (source_name)
        logs_sources_df = logs_orders_df.join(self.sources_df, logs_orders_df.source_id == self.sources_df.source_id, "left")

        # Step 7: Sort the logs by userId and timestamp to get the most recent source for each user
        logs_sources_df = logs_sources_df.orderBy("logs_userId", "logs_timestamp_ist", ascending=False)

        # Step 8: Define a window spec to get the most recent source per user (first row per user)
        window_spec = Window.partitionBy("logs_userId").orderBy(F.col("logs_timestamp_ist").desc())

        # Step 9: Get the most recent source for each user
        most_recent_sources_df = logs_sources_df.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1).drop("rank")

        # Step 10: Filter rows where an order (conversion) exists
        conversions_df = most_recent_sources_df.filter(F.col("orderId").isNotNull())

        # Step 11: Format the timestamp to include both date and time
        conversions_df = conversions_df.withColumn("date_time", F.date_format(F.col("logs_timestamp_ist"), "yyyy-MM-dd HH:mm:ss"))

        # Step 12: Extract just the date (ignoring time) for grouping
        conversions_df = conversions_df.withColumn("date_only", F.to_date(F.col("logs_timestamp_ist")))

        # Step 13: Group by the extracted date and source_name, collect all relevant info
        grouped_conversions_df = conversions_df.groupBy("date_only", "source_name").agg(
            F.collect_list(F.struct("date_time", "source_name", "orderId")).alias("conversions")
        )

        # Step 14: Create a new column `conversion_count` to count the number of conversions per date and source
        grouped_conversions_df = grouped_conversions_df.withColumn(
            "conversion_count", F.size(F.col("conversions"))
        )

        # Step 15: Format the output as needed (map each date to a dict of conversions)
        result_df = grouped_conversions_df.select(
            "date_only", "conversions", "conversion_count"
        )

        # Step 16: Collect the results into JSON strings
        results = result_df.toJSON().collect()

        # Step 17: Process the results into a new dict (this part depends on the logic of create_new_dict)
        new_data = self.create_new_dict(results)

        # Return the processed data
        return new_data