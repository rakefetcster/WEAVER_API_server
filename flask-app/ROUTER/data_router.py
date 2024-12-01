from flask import Blueprint,jsonify,request
from BLL.data_bll import DataBL
from BLL.data_panda_bll import DataPandaBll
import pandas as pd

data1 = Blueprint('data', __name__)
data_bl = DataBL()
data_panda = DataPandaBll()


@data1.route("/orders", methods=['GET'])
def get_orders_data():
    # print("order")
    orderds_df = data_bl.get_orders()
    # orderds_df.show() 
    # return jsonify(orderds_df)
    return orderds_df

@data1.route("/pagesvisit", methods=['GET'])
def get_visit_data():
    pagevisit_df = data_bl.get_enters()  
    # pagevisit_df.show()
    return pagevisit_df    

@data1.route("/visitors", methods=['GET'])
def get_visitors():
    # Capture query parameters from URL
    dateFrom = request.args.get('dateFrom')  # Default is None if the param is not provided
    dateTo = request.args.get('dateTo')      # Default is None if the param is not provided
    visitors_df = data_panda.get_vistors_by_filter(dateFrom,dateTo)
    return visitors_df    

@data1.route("/logs", methods=['GET'])
def get_logs_data():
    data = request.json
    orderds_df = data_panda.get_logs(data)
    # return jsonify(orderds_df)
    return orderds_df

@data1.route("/conversions", methods=['GET'])
def get_conversions_data():
    dateFrom = request.args.get('dateFrom')  # Default is None if the param is not provided
    dateTo = request.args.get('dateTo')      # Default is None if the param is not provided
    # print("order")
    orderds_df = data_bl.get_conversions_by_recent_source_and_date_new(dateFrom,dateTo)
    # orderds_df.show() 
    # return jsonify(orderds_df)
    return orderds_df