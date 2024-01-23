from kafka import KafkaProducer

import csv

import time

import json

from typing import Union


#function to read csv file
def read_csv(file_path):

    data = []

    with open(file_path, newline='', encoding='utf-8') as csvfile:

        reader = csv.DictReader(csvfile)

        for row in reader:

            data.append(row)

    return data

#flight Schema 
flight_schema = {
     'Flight Date':str ,
     'Flight Status': str,
     'Departure Airport': str,
     'Departure Scheduled': str,
     'Delay-dep':int,
     'Arrival Airport': str,
     'Arrival Scheduled': str,
     'Delay-arr':int,
     'Airline Name': str,
     'Flight Number': int,
     'Flight IATA': str,
     'Flight ICAO': str  
}

if __name__ == "__main__":

    bootstrap_servers = 'localhost:9092'



    # For Flight data
    flight_data_path = "flight_data.csv"
    #topic name 
    flight_topic = "flight"
    #read csv file using read_csv function
    flight_data = read_csv(flight_data_path)

   

    # Create a Kafka producer

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))



    # Send data to Kafka topic
    for line in flight_data:
        # Cast the record_flight values based on the flight_schema
        flight_data_dict = {}
        for key, value in flight_schema.items():
             raw_value = line[key]
             if raw_value != '' and raw_value != 'Delay-dep'and raw_value != 'Delay-arr' and raw_value != 'Flight Number':
                flight_data_dict[key] = value(raw_value)
             else:
                flight_data_dict[key] = None


        # Serialize the casted record and send it to Kafka
        producer.send(flight_topic, value=flight_data_dict)
        print(f"Sent message to {flight_topic}: {flight_data_dict}")

        time.sleep(1)

    # Flush and close the producer
    producer.flush()
    producer.close()




