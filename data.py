import requests
from datetime import datetime
import csv
from dotenv import load_dotenv
import os

api_key = os.getenv("API_KEY")
api_url = 'http://api.aviationstack.com/v1/flights'
# Url Api
url_with_key = f"{api_url}?access_key={api_key}"
response = requests.get(url_with_key)

if response.status_code == 200:
    data = response.json()['data']

    # init data_tuples
    data_tuples = []

    for flight in data:
        # Créer un dictionnaire avec les données de chaque vol
        flight_data = {
            'Flight Date': datetime.strptime(flight['flight_date'], '%Y-%m-%d'),
            'Flight Status': flight['flight_status'],
            'Departure Airport': flight['departure']['airport'],
            'Departure Scheduled': datetime.strptime(flight['departure']['scheduled'], '%Y-%m-%dT%H:%M:%S%z'),
            'Delay-dep':flight['departure']['delay'],
            'Arrival Airport': flight['arrival']['airport'],
            'Arrival Scheduled': datetime.strptime(flight['arrival']['scheduled'], '%Y-%m-%dT%H:%M:%S%z'),
            'Delay-arr':flight['arrival']['delay'],
            'Airline Name': flight['airline']['name'],
            'Flight Number': flight['flight']['number'],
            'Flight IATA': flight['flight']['iata'],
            'Flight ICAO': flight['flight']['icao']
        }

        # Add to data
        data_tuples.append(tuple(flight_data.values()))

    # Save data into CSV file
    csv_filename = 'flight_data.csv'
    with open(csv_filename, 'a', newline='') as file:
        writer = csv.writer(file)

        # write header to csv
        header = list(flight_data.keys())
        writer.writerow(header)

        # write data into csv
        writer.writerows(data_tuples)