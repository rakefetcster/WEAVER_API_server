from flask import Blueprint,jsonify
from BLL.data_bll import DataBL


data1 = Blueprint('data', __name__)
data_bl = DataBL()


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
