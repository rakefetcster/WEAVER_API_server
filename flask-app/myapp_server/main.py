from flask import Flask
import json
from bson import ObjectId
from flask_cors import CORS
from ROUTER.data_router import data

class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        # if isinstance(obj, (datetime, date)):
            #     return obj.isoformat()
        return json.JSONEncoder.default(self,obj)
    
app = Flask(__name__)
app.json_encoder = JSONEncoder
app.url_map.strict_slashes = False
CORS(app)
app.register_blueprint(data, url_prefix="/data")

app.run()
