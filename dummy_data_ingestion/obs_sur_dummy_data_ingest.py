import json
import os
from kafka import KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation

# Load configuration
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(os.path.join(config_path[0], "config.ini"))

# Kafka configuration
kafka_url = config.get("KAFKA", "url")
producer = KafkaProducer(bootstrap_servers=[kafka_url])

def send_data_to_kafka(data, datasource,topic, producer):
    try:
        # Convert data to appropriate format (e.g., json)a
        value = json.dumps(data).encode('utf-8')
        future = producer.send(topic, value=value)
        producer.flush()
        record_metadata = future.get()
        print(record_metadata)
        # Add error handling and delivery callbacks if needed
        print(f"successfully send data to {datasource} datasource")
    except Exception as e:
        print(f"failed to send data to {datasource}datasource")

# def process_json_file(file_path,producer):
#     with open(file_path, 'r') as f:
#         data = json.load(f)
#         # Extract desired columns or keys
#         extracted_data = data["columns"]  # Replace with your extraction logic
#         datasource = data['datasource']
#         topic = config.get("KAFKA", data["kafka_topic"])
#         send_data_to_kafka(extracted_data,datasource, topic, producer)

def process_json_file(file_path, producer):
    with open(file_path, 'r') as f:
        data = json.load(f)
        # Assuming data is a list of JSON objects
        for json_obj in data:
            extracted_data = json_obj["columns"]  # Replace with your extraction logic
            datasource = json_obj['datasource']
            topic = config.get("KAFKA", json_obj["kafka_topic"])
            send_data_to_kafka(extracted_data, datasource, topic, producer)

def main():
    # Kafka configuration
    kafka_url = config.get("KAFKA", "url")
    producer = KafkaProducer(bootstrap_servers=[kafka_url])
    util_folder = 'Util'
    file_path = os.path.join(util_folder, 'datasources.json')
    process_json_file(file_path,producer)
    producer.flush()
    producer.close()

if __name__ == '__main__':
    main()
