from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import boto3
from io import BytesIO
from botocore.exceptions import NoCredentialsError

# Global DataFrame to store all the weather data
weather_data_df = pd.DataFrame()

def kelvin_to_fahrenheit(kelvin_temp):
    fahrenheit_temp = (9 / 5) * (kelvin_temp - 273.15) + 32
    return fahrenheit_temp


def transform_load_data(city, **kwargs):
    global weather_data_df
    task_instance = kwargs['ti']  # Access task_instance from kwargs
    data = task_instance.xcom_pull(task_ids=f'extract_weather_data_{city}')
    
    city_name = data['name']
    weather_description = data['weather'][0]['description']
    temp_farhenheit = kelvin_to_fahrenheit(data['main']["temp"])
    feels_like_farhenheit = kelvin_to_fahrenheit(data['main']["feels_like"])
    min_temp_farhenheit = kelvin_to_fahrenheit(data['main']["temp_min"])
    max_temp_farhenheit = kelvin_to_fahrenheit(data['main']["temp_max"])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = pd.DataFrame([{
        'City': city_name,
        'Description': weather_description,
        'Feels Like (F)': feels_like_farhenheit,
        'Minimum Temp (F)': min_temp_farhenheit,
        'Maximum Temp (F)': max_temp_farhenheit,
        'Pressure': pressure,
        'Humidity': humidity,
        'Wind Speed': wind_speed,
        'Time of Record': time_of_record,
        'Sunrise (Local time)': sunrise_time,
        'Sunset (Local time)': sunset_time
    }])

    # Use pd.concat instead of append
    weather_data_df = pd.concat([weather_data_df, transformed_data], ignore_index=True)

    # Convert DataFrame to CSV in memory
    csv_buffer = BytesIO()
    weather_data_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)  # Reset buffer position to start

    # Upload the in-memory CSV to S3
    s3_bucket_name = "s3_bucket_name"
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    s3_filename = f"current_weather_data_{dt_string}.csv"

    try:
        s3_client = boto3.client('s3', aws_access_key_id="aws_access_key",
                                 aws_secret_access_key="aws_secret_access_key")
        s3_client.upload_fileobj(csv_buffer, s3_bucket_name, s3_filename)
        print(f"File uploaded successfully to s3://{s3_bucket_name}/{s3_filename}")
    except NoCredentialsError:
        print("Credentials not available.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 30),
    'email': ['adi4733@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def get_cities():
    with open('/home/ubuntu/cities.txt', 'r') as file:
        cities = file.read().splitlines()
    return cities

with DAG('Weather_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    cities = get_cities()

    for city in cities:
        is_weather_api_ready = HttpSensor(
            task_id=f'is_weather_api_ready_{city}',
            http_conn_id='weathermap_api',
            endpoint=f'/data/2.5/weather?q={city}&appid=d10fdf0492c79652445f9c42b8e67ac2'
        )

        extract_weather_data = SimpleHttpOperator(
            task_id=f'extract_weather_data_{city}',
            http_conn_id='weathermap_api',
            endpoint=f'/data/2.5/weather?q={city}&appid=d10fdf0492c79652445f9c42b8e67ac2',
            method='GET',
            response_filter=lambda r: json.loads(r.text),
            log_response=True
        )

        transform_load_weather_data = PythonOperator(
        task_id=f'transform_load_weather_data_{city}',
        python_callable=transform_load_data,
        op_args=[city],  # Pass city as op_args
        provide_context=True  # Provide context to access kwargs
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
