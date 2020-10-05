# Riley Busche 2020-10-5
# Uses Cloudwatch to get S3 bucket size and formats to CSV
import boto3
import csv
from datetime import datetime
import pprint
import os

file_name = 'test_file.csv'

def get_bucket_size(bucket_name : str, cloudwatch_client) -> str:    
    response = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'test',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/S3',
                        'MetricName': 'BucketSizeBytes',
                        'Dimensions': [
                            {
                                'Name': 'BucketName',
                                'Value': bucket_name
                            },
                            {
                                'Name': 'StorageType',
                                'Value': 'StandardStorage'
                            }
                        ]
                    },
                    'Period': 60,
                    'Stat': 'Average',
                    # 'Unit': 'Seconds'|'Microseconds'|'Milliseconds'|'Bytes'|'Kilobytes'|'Megabytes'|'Gigabytes'|'Terabytes'|'Bits'|'Kilobits'|'Megabits'|'Gigabits'|'Terabits'|'Percent'|'Count'|'Bytes/Second'|'Kilobytes/Second'|'Megabytes/Second'|'Gigabytes/Second'|'Terabytes/Second'|'Bits/Second'|'Kilobits/Second'|'Megabits/Second'|'Gigabits/Second'|'Terabits/Second'|'Count/Second'|'None'
                },
                # 'Expression': 'string',
                # 'Label': 'string',
                'ReturnData': True,
                # 'Period': 123
            },
        ],
        StartTime=datetime(2020, 8, 1),
        EndTime=datetime.now(),
        # NextToken='string',
        # ScanBy='TimestampDescending'|'TimestampAscending',
        # MaxDatapoints=123
    )

    bucket_size = str(response['MetricDataResults'][0]['Values'][0])

    return bucket_size

def new_csv():
    os.remove(file_name)

def write_row(values : list):
    with open(file_name, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(values)

if __name__ == "__main__":
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')
    s3_resource = boto3.resource('s3')

    new_csv()

    for bucket in s3_resource.buckets.all():
        print(bucket.name)
        try:
            bucket_size = get_bucket_size(bucket.name, cloudwatch_client)
            values = [bucket.name, bucket_size]
        except:
            values = [bucket.name, 'null']
        write_row(values)
