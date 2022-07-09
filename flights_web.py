import streamlit as st
import sqlite3
import pandas as pd

conn = sqlite3.connect('flights.sqlite3')
c = conn.cursor()

resp_flights_df = pd.read_sql_query("SELECT * FROM flight", conn)

st.dataframe(resp_flights_df)