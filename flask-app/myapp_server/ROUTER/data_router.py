from flask import Blueprint
from BLL.data_bll import DataBL


data = Blueprint('data', __name__)
data_bl = DataBL()


@data.route("/orders", methods=['GET'])
def get_orders_data():
    orderds_df = data_bl.get_orders()  
    return orderds_df
@data.route("/pagesvisit", methods=['GET'])
def get_visit_data():
    pagevisit_df = data_bl.get_enters()  
    return pagevisit_df    
