How to Run
Prerequisites:
Install dependencies: pip install kafka-python pymongo pyspark requests bokeh flask python-dateutil
Ensure Kafka, Zookeeper, and MongoDB are running locally.
Create Kafka topics: RawSensorData and CleanSensorData.
Run the Script:
Save the code as sensor_data_pipeline.py.
Run it with Bokeh server: bokeh serve --show sensor_data_pipeline.py
Access the Visualization:
Open the Bokeh server URL (usually http://localhost:5006/sensor_data_pipeline) to see the real-time plot.
What Happens:
Flask API generates simulated sensor data at http://localhost:3030/sensordata.
Producer fetches data and sends it to RawSensorData.
Processor consumes RawSensorData, validates it, stores it in MongoDB, and sends clean data to CleanSensorData.
Bokeh consumes CleanSensorData and updates the plot every second.
