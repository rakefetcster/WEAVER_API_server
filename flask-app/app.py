from flask import Flask
from pyspark.sql import SparkSession

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Flask app with PySpark!"

@app.route('/spark')
def spark():
    spark = SparkSession.builder \
        .appName("Flask and PySpark") \
        .getOrCreate()
    
    # Example Spark DataFrame
    df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ["id", "name"])
    data = df.collect()  # Collect data to return as a simple response
    
    return str(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)  # Run Flask app
