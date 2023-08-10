from kafka import KafkaProducer
import csv
from time import sleep
import json
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor


# Kafka broker address
bootstrap_servers = '10.18.17.153:9092'
# Kafka topics to send the data
topic_LF = "Heelstrike_LF"
topic_LH = "Heelstrike_LH"
topic_LS = "Heelstrike_LS"
topic_RF = "Heelstrike_RF"
topic_RH = "Heelstrike_RH"
topic_RS = "Heelstrike_RS"
topic_SA = "Heelstrike_SA"

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Load the CSV files into DataFrames
csv_file_LF = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/LF_all_10.csv"
csv_file_LH = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/LH_all_10.csv"
csv_file_LS = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/LS_all_10.csv"
csv_file_RF = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/RF_all_10.csv"
csv_file_RH = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/RH_all_10.csv"
csv_file_RS = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/RS_all_10.csv"
csv_file_SA = "/home/ayemon/KafkaProjects/kafkaspark07_90/heelstrike/heelstrike_10/SA_all_10.csv"


def send_csv_to_kafka_with_label(csv_file, topic):
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Get the header
        for row in csv_reader:
            # Assuming each row is a list of numerical values
            message = dict(zip(header, row))
            producer.send(topic, value=message)
            print(f"Sending data to topic '{topic}': {message}")
            sleep(2)

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=7) as executor:
        future1 = executor.submit(send_csv_to_kafka_with_label, csv_file_LF, topic_LF)
        future2 = executor.submit(send_csv_to_kafka_with_label, csv_file_LH, topic_LH)
        future3 = executor.submit(send_csv_to_kafka_with_label, csv_file_LS, topic_LS)
        future4 = executor.submit(send_csv_to_kafka_with_label, csv_file_RF, topic_RF)
        future5 = executor.submit(send_csv_to_kafka_with_label, csv_file_RH, topic_RH)
        future6 = executor.submit(send_csv_to_kafka_with_label, csv_file_RS, topic_RS)
        future7 = executor.submit(send_csv_to_kafka_with_label, csv_file_SA, topic_SA)

        # Wait for both tasks to complete
        concurrent.futures.wait([future1, future2, future3, future4, future5, future6, future7])

    producer.close()