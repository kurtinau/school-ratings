import requests
import csv
import boto3
import os
from io import StringIO
from datetime import datetime

date_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

def lambda_handler(event, context):
    # Set the API endpoint URL
    endpoint_url_metadata = "https://api.data.abs.gov.au/data/ABS,ABS_REGIONAL_LGA2021,1.0.0/HOUSES_3...A?startPeriod=2011&dimensionAtObservation=AllDimensions"

    # Set the request headers
    headers = {
        "Accept": "application/vnd.sdmx.data+csv"
    }

    # Make the API request
    response = requests.get(endpoint_url_metadata, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        csv_data = csv.reader(response.text.splitlines())
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(csv_data)
        # Save the CSV file to S3 bucket
        s3 = boto3.client('s3')
        bucket_name = 'school-rating-raw-data'
        file_key = f'ABS/house-price/{date_time}/median_house_price_by_LGA_2016_2021.csv'  
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_key)
    else:
        # Print the error message
        print("Error:", response.text)


