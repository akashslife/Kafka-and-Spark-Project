Real-Time Sensor Data Pipeline
A real-time data pipeline that ingests, validates, processes, and stores environmental sensor data using Apache Kafka, Apache Spark, and MongoDB. The system performs data cleaning and validation before storing cleaned records and pushing them to another Kafka topic.

🛠️ Tech Stack
Apache Kafka – Message broker for real-time data streaming

Apache Spark – Distributed data processing

MongoDB – NoSQL database for storing validated sensor data

Python – Core language for pipeline scripting

PyMongo – MongoDB driver for Python

dateutil – Parsing timestamp strings

bson – MongoDB JSON serializer

📦 Features
Consumes raw sensor data from Kafka (RawSensorData topic)

Validates and cleans:

TimeStamp

WaterTemperature (flags sensor malfunctions if out of range)

Turbidity

BatteryLife

Beach name

Measurement ID

Avoids duplication by checking MongoDB for existing timestamps

Stores clean data in MongoDB

Produces validated data to Kafka (CleanSensorData topic)

📁 Project Structure
perl
Copy code
real-time-data-pipeline/
│
├── pipeline.py             # Main script for Kafka -> Spark -> MongoDB flow
├── README.md               # You're here!
🔄 Sample Data Format
Raw data consumed from Kafka is a single-line string:

yaml
Copy code
2025-04-12T10:34:00Z 23.5 1200.2 87.5 BeachA Beach12345
After validation, the cleaned structure looks like:

json
Copy code
{
  "RawData": "2025-04-12T10:34:00Z 23.5 1200.2 87.5 BeachA Beach12345",
  "TimeStamp": "2025-04-12T10:34:00Z",
  "WaterTemperature": 23.5,
  "Turbidity": 1200.2,
  "BatteryLife": 87.5,
  "Beach": "BeachA",
  "MeasurementID": 12345
}
🚀 How to Run
✅ Prerequisites
Ensure the following are installed and running:

Apache Kafka (zookeeper & kafka-server)

MongoDB (mongod)

Python 3.x

Kafka topics: RawSensorData, CleanSensorData

📦 Install Dependencies
bash
Copy code
pip install kafka-python pymongo python-dateutil
🔁 Start the Pipeline
bash
Copy code
python pipeline.py
🧪 Example Usage
You can produce test data into the Kafka topic:

bash
Copy code
kafka-console-producer.sh --broker-list localhost:9092 --topic RawSensorData
Paste:

yaml
Copy code
2025-04-12T10:34:00Z 23.5 1200.2 87.5 BeachA Beach12345
📚 Future Improvements
Add unit tests for validation logic

Real-time dashboard using Grafana or Streamlit

Integration with Apache Flink for more advanced stream processing

