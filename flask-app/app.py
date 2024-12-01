from flask import Flask
import json
from bson import ObjectId
from flask_cors import CORS
from ROUTER.data_router import data1

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        # if isinstance(obj, (datetime, date)):
            #     return obj.isoformat()
        return json.JSONEncoder.default(self,obj)

  
    
app = Flask(__name__)
# app.json_encoder = JSONEncoder
app.url_map.strict_slashes = False
# CORS(app, resources={r"/*": {"origins": "http://localhost:3003"}}) 
CORS(app)
app.register_blueprint(data1, url_prefix="/data")

app.run(host='0.0.0.0', port=5002)