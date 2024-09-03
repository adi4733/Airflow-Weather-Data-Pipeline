# Airflow-Weather-Data-Pipeline

## Overview
This Apache Airflow Directed Acyclic Graph (DAG) is designed to extract weather data for Portland from the OpenWeatherMap API, transform the data, and load it into an AWS S3 bucket as a CSV file.

## The DAG performs the following steps:

**Sensor Check:** Ensures that the weather API is available before attempting to fetch data.
**Data Extraction:** Retrieves weather data from the OpenWeatherMap API.
**Data Transformation and Loading:** Converts the temperature from Kelvin to Fahrenheit, formats the data, and uploads it to an S3 bucket.


## Ensure that the following Python packages are installed:

apache-airflow
pandas
requests
boto3 (if you plan to interact with AWS directly in your environment)

## Configuration
**OpenWeatherMap API Key:** Replace the API key placeholder in the extract_weather_data and is_weather_api_ready tasks with your actual OpenWeatherMap API key.
**AWS Credentials:** Configure your AWS credentials for S3 access. Ensure you provide the correct key, secret, and token in the aws_credentials dictionary.


## Explanation
HttpSensor Task (is_weather_api_ready): Checks if the weather API is ready before proceeding.
SimpleHttpOperator Task (extract_weather_data): Fetches weather data from the OpenWeatherMap API.
PythonOperator Task (transform_load_weather_data): Transforms and loads the weather data into an AWS S3 bucket in CSV format.

## How to Run
Ensure your Airflow environment is properly configured.
Place the script in the dags folder of your Airflow setup.
Trigger the DAG either manually from the Airflow UI or let it run based on its schedule.
