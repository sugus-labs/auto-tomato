import requests
import pandas as pd
import sqlite3

params = {
  'access_key': '2c7569c526ba8294d56ca17e0e88bc12',
  'dep_iata': ["BIO"]
}

api_result = requests.get('http://api.aviationstack.com/v1/flights', params)
api_response = api_result.json()

flight_lst = []

for flight in api_response["data"]:

    flight_dict = {}

    flight_dict['flight_date'] = flight['flight_date']
    flight_dict['departure_airport'] = flight['departure']['airport']
    flight_dict['departure_terminal'] = flight['departure']['terminal']
    flight_dict['departure_gate'] = flight['departure']['gate']
    flight_dict['departure_delay'] = flight['departure']['delay']
    flight_dict['departure_scheduled'] = flight['departure']['scheduled']
    flight_dict['departure_estimated'] = flight['departure']['estimated']
    flight_dict['arrival_airport'] = flight['arrival']['airport']
    flight_dict['arrival_terminal'] = flight['arrival']['terminal']
    flight_dict['arrival_gate'] = flight['arrival']['gate']
    flight_dict['arrival_delay'] = flight['arrival']['delay']
    flight_dict['arrival_scheduled'] = flight['arrival']['scheduled']
    flight_dict['arrival_estimated'] = flight['arrival']['estimated']
    flight_dict['airline_name'] = flight['airline']['name']
    flight_dict['flight_number'] = flight['flight']['number']

    flight_lst.append(flight_dict)

flights_df = pd.DataFrame(
    flight_lst, 
    columns = ['flight_date', 'departure_airport', 'departure_terminal', 
        'departure_gate', 'departure_delay', 'departure_scheduled', 
        'departure_estimated', 'arrival_airport', 'arrival_terminal', 
        'arrival_gate', 'arrival_delay', 'arrival_scheduled', 'arrival_estimated', 
        'airline_name', 'flight_number'])
flights_df["flight_date"] = pd.to_datetime(flights_df["flight_date"])
flights_df["departure_scheduled"] = pd.to_datetime(flights_df["departure_scheduled"])
flights_df["departure_estimated"] = pd.to_datetime(flights_df["departure_estimated"])
flights_df["arrival_scheduled"] = pd.to_datetime(flights_df["arrival_scheduled"])
flights_df["arrival_estimated"] = pd.to_datetime(flights_df["arrival_estimated"])

conn = sqlite3.connect("flights.sqlite3")
c = conn.cursor()

create_flights_query = """
    CREATE TABLE IF NOT EXISTS flight (
        [index] TEXT,
        [flight_date] TEXT,
        [departure_airport] TEXT,
        [departure_terminal] TEXT,
        [departure_gate] TEXT,
        [departure_delay] REAL,
        [departure_scheduled] TEXT,
        [departure_estimated] TEXT,
        [arrival_airport] TEXT,
        [arrival_terminal] TEXT,
        [arrival_gate] TEXT,
        [arrival_delay] REAL,
        [arrival_scheduled] TEXT,
        [arrival_estimated] TEXT,
        [airline_name] TEXT,
        [flight_number] TEXT        
    );
"""

c.execute(create_flights_query)           
conn.commit()

flights_df.to_sql('flight', con = conn, if_exists = 'append')