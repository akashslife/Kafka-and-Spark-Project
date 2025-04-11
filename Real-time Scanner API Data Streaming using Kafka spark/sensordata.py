import json
from bson import json_util
from dateutil import parser
from pyspark import SparkContext
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

# Initialize MongoDB
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['RealTimeDB']
collection = db['RealTimeCollection']

# Initialize Spark Context
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

# Kafka Consumer and Producer
consumer = KafkaConsumer(
    'RawSensorData',
    auto_offset_reset='earliest',
    bootstrap_servers=['localhost:9092'],
    consumer_timeout_ms=1000
)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


def timestamp_exists(timestamp):
    return collection.count_documents({'TimeStamp': timestamp}) > 0


def parse_sensor_data(msg):
    raw_data = msg.value.decode("utf-8")
    tokens = sc.parallelize(raw_data.split()).collect()

    data = {
        "RawData": raw_data,
        "TimeStamp": "Error",
        "WaterTemperature": "Error",
        "Turbidity": "Error",
        "BatteryLife": "Error",
        "Beach": "Error",
        "MeasurementID": "Error"
    }

    try:
        data["TimeStamp"] = parser.isoparse(tokens[0])
    except Exception:
        pass

    try:
        water_temp = float(tokens[1])
        data["WaterTemperature"] = water_temp if -10 <= water_temp <= 99 else "Sensor Malfunctions"
    except Exception:
        pass

    try:
        turbidity = float(tokens[2])
        data["Turbidity"] = turbidity if turbidity <= 5000 else "Sensor Malfunctions"
    except Exception:
        pass

    try:
        data["BatteryLife"] = float(tokens[3])
    except Exception:
        pass

    try:
        data["Beach"] = tokens[4]
    except Exception:
        pass

    try:
        data["MeasurementID"] = int(tokens[5].replace("Beach", ""))
    except Exception:
        pass

    return data


def process_messages():
    for msg in consumer:
        if msg.value.decode("utf-8") == "Error in Connection":
            continue

        parsed_data = parse_sensor_data(msg)

        if not timestamp_exists(parsed_data['TimeStamp']):
            collection.insert_one(parsed_data)
            producer.send(
                "CleanSensorData",
                json.dumps(parsed_data, default=json_util.default).encode('utf-8')
            )
        print(parsed_data)


if __name__ == "__main__":
    process_messages()
