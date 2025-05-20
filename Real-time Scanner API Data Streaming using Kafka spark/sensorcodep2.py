# Import necessary libraries
import time
import random
import json
from datetime import datetime
import pytz
import pandas as pd
# from typing import Optional, Dict, Any
from flask import Flask, Response
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pyspark.sql import SparkSession
from dateutil import parser
from bokeh.plotting import figure, curdoc
from bokeh.models import ColumnDataSource, DatetimeTickFormatter, Div
from bokeh.layouts import row
import requests
# from threading import Thread
# from flask_cors import CORS
# from flask import Flask, jsonify # to allow cross-origin requests

# Configuration constants
KAFKA_BOOTSTRAP_SERVERS =['localhost:9092']
RAW_TOPIC = 'RawSensorData'
CLEAN_TOPIC ='CleanSensorData'
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'RealTimeDB'#, "pymongo"
COLLECTION_NAME = 'RealTimeCollection'
# API_URL = (if data in apis(Apidata.py) not in ret_url="Apidata.py")# i am hiding the senc. data
UPDATE_INTERVAL = 1000  # Bokeh update interval in ms
ROLLOVER = 10  # Number of displayed data points 
TIMEZONE = pytz.timezone('Asia/Calcutta')#timezone for datetime 

# Flask app for simulating sensor data
app = Flask(__name__)

@app.route('/sensordata')
def get_sensor_data() -> Response:
    """Generate simulated sensor data for the API."""
    beach = 'Montrose_Beach'
    timestamp = datetime.now().isoformat()
    water_temperature = round(random.uniform(0.0, 31.5), 2)
    turbidity = round(random.uniform(0.0, 1683.48), 2)
    battery_life = round(random.uniform(4.8, 13.3), 2)
    measurement_id = random.randint(10000, 999999)
    
    response = f"{timestamp} {water_temperature} {turbidity} {battery_life} {beach} {measurement_id}"
    return Response(response, mimetype='text/plain')

# Kafka Producer for sending raw sensor data
def produce_sensor_data():
    """Fetch sensor data from API and send to Kafka RawSensorData topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode('utf-8')
    )
    
    while True:
        try:
            response = requests.get(API_URL, timeout=5)
            response.raise_for_status()
            data = response.text
            producer.send(RAW_TOPIC, data)
            print(f"Sent to {RAW_TOPIC}: {data}")
        except requests.RequestException as e:
            print(f"Error fetching sensor data: {e}")
        time.sleep(1)

# Kafka Consumer and Processor for cleaning and storing data
def initialize_spark() -> SparkSession:
    """Initialize Spark session for data processing."""
    return SparkSession.builder \
        .appName("SensorDataProcessor") \
        .config("spark.log.level", "WARN") \
        .getOrCreate()

def initialize_mongo() -> MongoClient:
    """Initialize MongoDB client."""
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME]

def timestamp_exists(collection: MongoClient, timestamp: Any) -> bool:
    """Check if a timestamp already exists in MongoDB."""
    return collection.count_documents({'TimeStamp': timestamp}) > 0

def validate_and_structure_data(msg: str, spark: SparkSession) -> Dict[str, Any]:
    """Validate and structure sensor data."""
    data_dict = {"RawData": msg}
    fields = msg.split()

    try:
        data_dict["TimeStamp"] = parser.isoparse(fields[0])
    except (IndexError, ValueError):
        data_dict["TimeStamp"] = "Error"

    try:
        water_temp = float(fields[1])
        data_dict["WaterTemperature"] = water_temp if -10 <= water_temp <= 99 else "Sensor Malfunctions"
    except (IndexError, ValueError):
        data_dict["WaterTemperature"] = "Error"

    try:
        turbidity = float(fields[2])
        data_dict["Turbidity"] = turbidity if turbidity <= 5000 else "Sensor Malfunctions"
    except (IndexError, ValueError):
        data_dict["Turbidity"] = "Error"

    try:
        data_dict["BatteryLife"] = float(fields[3])
    except (IndexError, ValueError):
        data_dict["BatteryLife"] = "Error"

    try:
        data_dict["Beach"] = str(fields[4])
    except IndexError:
        data_dict["Beach"] = "Error"

    try:
        data_dict["MeasurementID"] = int(fields[5].replace("Beach", ""))
    except (IndexError, ValueError):
        data_dict["MeasurementID"] = "Error"

    return data_dict

def process_sensor_data():
    """Consume raw data, process it, store in MongoDB, and send to CleanSensorData topic."""
    spark = initialize_spark()
    collection = initialize_mongo()
    
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=lambda o: str(o)).encode('utf-8')
    )
    
    for msg in consumer:
        if msg.value.decode("utf-8") != "Error in Connection":
            data = validate_and_structure_data(msg.value.decode("utf-8"), spark)
            if not timestamp_exists(collection, data['TimeStamp']):
                collection.insert_one(data)
                producer.send(CLEAN_TOPIC, data)
                print(f"Processed and sent data: {data}")

# Bokeh Visualization
def initialize_plot():
    """Initialize Bokeh plot and data source."""
    source = ColumnDataSource({"x": [], "y": []})
    
    p = figure(
        title="Water Temperature Sensor Data",
        x_axis_type="datetime",
        plot_width=1000,
        plot_height=400
    )
    p.line("x", "y", source=source)
    
    p.xaxis.formatter = DatetimeTickFormatter(hours=["%H:%M"])
    p.xaxis.axis_label = 'Time'
    p.yaxis.axis_label = 'Water Temperature (°C)'
    p.title.align = "center"
    p.title.text_color = "orange"
    p.title.text_font_size = "25px"
    
    div = Div(text="", width=200, height=35)
    return source, p, div

def update(source: ColumnDataSource, div: Div):
    """Update Bokeh plot with clean sensor data."""
    consumer = KafkaConsumer(
        CLEAN_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    for msg in consumer:
        data = msg.value
        try:
            timestamp = pd.to_datetime(data["TimeStamp"])
            water_temp = data["WaterTemperature"]
            if isinstance(water_temp, (int, float)):
                source.stream({"x": [timestamp], "y": [water_temp]}, ROLLOVER)
                div.text = f"TimeStamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                print(f"Plotted: {timestamp}, {water_temp}")
        except (KeyError, ValueError) as e:
            print(f"Error processing data for plot: {e}")
        break

def run_bokeh():
    """Set up and run Bokeh visualization."""
    source, plot, div = initialize_plot()
    doc = curdoc()
    doc.add_root(row(children=[div, plot]))
    doc.add_periodic_callback(lambda: update(source, div), UPDATE_INTERVAL)

def main():
    """Main function to run all components in separate threads."""
    # Run Flask app in a separate thread
    flask_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=3030, debug=False))
    flask_thread.daemon = True
    flask_thread.start()

    # flask_thread = Thread(target=lambda: app.run(host='8.0.8.0', port=3030, debug=True))
    # if __name__ == '__main__':
    #     flask_thread = Thread(target=lambda: app.run(host='https://s3.aws-c', port=3030, debug=True))
    # flask_thread.daemon = True
    # flask_thread.start()
    # Run Kafka producer in a separate thread
    producer_thread = Thread(target=produce_sensor_data)
    producer_thread.daemon = True
    producer_thread.start()
    
    # Run Kafka consumer and processor in a separate thread
    processor_thread = Thread(target=process_sensor_data)
    processor_thread.daemon = True
    processor_thread.start()
    
    # Run Bokeh visualization in the main thread (Bokeh server requirement)
    run_bokeh()

if __name__ == '__main__':
    main()