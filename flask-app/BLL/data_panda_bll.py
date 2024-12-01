from BLL.data_bll import DataBL
import pandas as pd
from flask import jsonify
from datetime import datetime
from pandas import json_normalize

data_bl = DataBL()

class DataPandaBll:
    def __init__(self):
        pass
    
    def get_vistors_by_filter(self, start, end):
        # Fetch data from the BLL (Business Logic Layer)
        visitors_df = data_bl.get_visitors_by_session_new()
        # Convert PySpark DataFrame to Pandas DataFrame
        pandas_df = visitors_df.toPandas()
        # Convert 'date' column to datetime format
        pandas_df['date'] = pd.to_datetime(pandas_df['date'])
        # Convert start and end dates to pandas datetime format for comparison
        start_date = pd.to_datetime(start)
        end_date = pd.to_datetime(end)
        # Filter the DataFrame to include only rows where 'date' is between start_date and end_date (inclusive)
        df_filtered = pandas_df[(pandas_df['date'] >= start_date) & (pandas_df['date'] <= end_date)]
        # Sort the filtered DataFrame by 'date'
        df_sorted = df_filtered.sort_values(by='date', ascending=True)
        # Convert the sorted DataFrame to a JSON format that Flask can handle
        return jsonify(df_sorted.to_dict(orient="records"))


    def get_logs1(self, data):
        timestamp_data = data['timestamp']
        logsDf = data_bl.get_logs()
        #  # Convert to DataFrame by normalizing the 'full_session' field
        df = json_normalize(logsDf, 'full_session', 
                            ['Campaign name', 'landing_url', 'referrer', 'userId'], 
                            errors='ignore', 
                            meta_prefix='parent_')  # Add prefix to avoid column name conflict
        
        # # Extract conditions
        # if 'conditions' in timestamp_data['timestamp']:
        #     operator = timestamp_data['timestamp'].get('operator', 'OR')
        #     conditions = timestamp_data['timestamp']['conditions']
            
        #     # Extract the 'dateFrom' and 'dateTo' values
        #     date_from_list = [condition.get('dateFrom') for condition in conditions]
        #     date_to_list = [condition.get('dateTo') for condition in conditions]
            
        #     # Convert to datetime
        #     date_from_list = pd.to_datetime(date_from_list)
        #     date_to_list = pd.to_datetime(date_to_list)

        #     # Filter the DataFrame based on the timestamp
        #     filtered_df = pd.DataFrame()
        #     for i in range(len(date_from_list)):
        #         # Apply the condition: Check if the timestamp is between dateFrom and dateTo
        #         condition_mask = (df['timestamp'] >= date_from_list[i]) & (df['timestamp'] <= date_to_list[i])
                
        #         # Since we're using 'OR', combine the conditions
        #         if i == 0:
        #             filtered_df = df[condition_mask]
        #         else:
        #             filtered_df = pd.concat([filtered_df, df[condition_mask]])

        #     # Drop duplicates after applying OR logic
        #     filtered_df = filtered_df.drop_duplicates()

                
        # else:
        #     pass
       
        
        # # Convert 'timestamp' outside of 'full_session' to datetime for filtering
        # df['parent_timestamp'] = pd.to_datetime(df['timestamp'], utc=True)  # Store the outer timestamp separately
        
        # # Define your time range for filtering the outer timestamp
        # start_date = pd.to_datetime(start).tz_localize('UTC')  # Make start date UTC-aware
        # end_date = pd.to_datetime(end).tz_localize('UTC')      # Make end date UTC-aware
        
        # # Filter the data to include only records within the specified time range for the outer timestamp
        # filtered_df = df[(df['parent_timestamp'] >= start_date) & (df['parent_timestamp'] <= end_date)]
        
        # Now, we need to reconstruct the original JSON structure, keeping the sessions intact
        # final_output = []

        # for _, row in filtered_df.iterrows():
        #     final_output.append({
        #         "Campaign name": row['Campaign name'],
        #         "full_session": [
        #             {
        #                 "Campaign name": row['Campaign name'],
        #                 "landing_url": row['landing_url'],
        #                 "referrer": row['referrer'],
        #                 "timestamp": row['timestamp'],  # Keeping the timestamp as it was in the session
        #                 "userId": row['userId']
        #             }
        #         ],
        #         "landing_url": row['landing_url'],
        #         "referrer": row['referrer'],
        #         "timestamp": row['parent_timestamp'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),  # Parent timestamp format
        #         "userId": row['userId']
        #     })
        
        return jsonify(logsDf)
    
    def get_logs(self, data):
        timestamp_data = data['timestamp']
        logsDf = data_bl.get_logs()
        #  # Convert to DataFrame by normalizing the 'full_session' field
        df = json_normalize(logsDf, 'full_session', 
                            ['Campaign name', 'landing_url', 'referrer', 'userId'], 
                            errors='ignore', 
                            meta_prefix='parent_')  # Add prefix to avoid column name conflict
        return jsonify(logsDf)
