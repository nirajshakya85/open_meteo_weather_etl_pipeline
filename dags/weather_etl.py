from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import requests
import json

# Latitue and Longitude for New York City
LATITUDE = '40.7128'
LONGITUDE = '-74.0060'
# Postgres connection ID
POST_GRES_CONN_ID = 'postgres_default'
# API connection ID
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 9, 26),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    @task
    def extract_weather_data():
        
        # Extracts weather data from the Open-Meteo API for New York City.
        
        # Use HttpHook to make the API request
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        
        # Build the API endpoint URL
        # https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060&current_weather=true
        endpoint = f'v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        # Make the API request
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task
    def transform_weather_data(weather_data):
        # Transforms the extracted weather data to extract relevant fields.
        current_weather = weather_data.get('current_weather', {})
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather.get('temperature'),
            'windspeed': current_weather.get('windspeed'),
            'winddirection': current_weather.get('winddirection'),
            'weathercode': current_weather.get('weathercode'),
            'time': current_weather.get('time')
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):
        # Loads the transformed weather data into a Postgres database.
        
        # Use PostgresHook to connect to the Postgres database
        pg_hook = PostgresHook(postgres_conn_id=POST_GRES_CONN_ID)

        #create the table if it doesn't exist
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                temperature DECIMAL(5, 2),
                windspeed DECIMAL(5, 2),
                winddirection INTEGER,
                weathercode INTEGER,
                time TIMESTAMP
            )
        """
        pg_hook.run(create_table_sql)

        # Insert the transformed data into the weather_data table
        insert_sql = """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        pg_hook.run(insert_sql, parameters=(
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode'],
            transformed_data['time']
        ))

    # Define the task dependencies
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
