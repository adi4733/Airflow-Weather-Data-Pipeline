from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import json


def kelvin_to_fahrenheit(kelvin_temp):
    fahrenheit_temp = (9 / 5) * (kelvin_temp - 273.15) + 32
    return fahrenheit_temp


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
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

    transformed_data = {
        'City': city,
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
    }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    aws_credentials = {
        "key": "",
        "secret": "",
        "token": ""
    }
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://weatherairflow07/{dt_string}.csv", index=False, storage_options=aws_credentials)




default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date':datetime(2024,8,30),
    'email':['adi4733@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=1)
}

with DAG('Weather_dag',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=d10fdf0492c79652445f9c42b8e67ac2'
        )

        extract_weather_data = SimpleHttpOperator(
        task_id= 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Portland&appid=d10fdf0492c79652445f9c42b8e67ac2',
        method = 'GET',
        response_filter = lambda r:json.loads(r.text),
        log_response = True
        )

        transform_load_weather_data  = PythonOperator(
        task_id = 'transform_load_weather_data',
        python_callable = transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >>transform_load_weather_data
