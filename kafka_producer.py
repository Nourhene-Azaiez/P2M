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
     'dt_iso': str,
     'Flight Date':str ,
     'Flight Status': str,
     'Departure Airport': str,
     'Departure Scheduled': str,
     'Delay-dep': Union[None,int],
     'Arrival Airport': str,
     'Arrival Scheduled': str,
     'Delay-arr':Union[None,int],
     'Airline Name': str,
     'Flight Number': int,
     'Flight IATA': str,
     'Flight ICAO': str  
}

if __name__ == "__main__":

    bootstrap_servers = 'localhost:9093'



    # For Flight data

    flight_data_path = "flight_data.csv"

    flight_topic = "flight"

    flight_data = read_csv(flight_data_path)

   

    # Sort flight data by timestamp

    flight_data = sorted(flight_data, key=lambda x: (x['dt_iso']))



    # Create a Kafka producer

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))



    # Send data to both Kafka topics simultaneously

    for record_flight in flight_data:

        # Send flight data for the same timestamp
        casted_record_flight = {key: flight_schema[key](record_flight[key]) if record_flight[key] != '' else None for key in flight_schema}

        casted_record_flight_serializable = {key: value if value is None or isinstance(value, (int, float, str, bool, list, dict)) else str(value) for key, value in casted_record_flight.items()}

        producer.send(flight_topic, value=casted_record_flight_serializable)

        print(f"Sent message to {flight_topic}: {casted_record_flight_serializable}")

        time.sleep(4)



    # Flush and close the producer

    producer.flush()

    producer.close()