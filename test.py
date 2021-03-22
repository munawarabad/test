import botocore
import boto3
# import json
# import datetime as dt
import pandas as pd
# import sys
import s3fs
from requests.auth import HTTPBasicAuth
import requests
# import uuid
from io import StringIO
import io
import csv


def get_s3_client(aws_key, aws_sercet):
  try:
    s3_client = boto3.client(
                             's3',
                             aws_access_key_id=aws_key,
                             aws_secret_access_key=aws_sercet 
                             )
  except botocore.exceptions.ClientError as e:
    print(e.response)
  return s3_client

def check_bucket(client, bucket_name):
  try:
    client.head_bucket(Bucket=bucket_name)
    return True
  except botocore.exceptions.ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
      print("Bucket doesn't exists")
      return False
    else:
      print("Unable to query aws")
      raise SystemExit(e)


def create_ped_bucket(client, bucket_name):
  if check_bucket(client, bucket_name):
      print("Bucket Already exists")
      return True
  else:        
    try:
      location = {"LocationConstraint": "ap-southeast-2"}
      client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
      print("New bucket {} created".format(bucket_name))
      return True
    except ClientError as e:
      print("Unable to query aws")
      raise SystemExit(e)


def ingest_data(bucket_name, aws_key, aws_sercet, sodas_key, sodas_key_secret):
  
  headers = {"Accept": "application/json"}
  auth = HTTPBasicAuth(sodas_key, sodas_key_secret)
  url = "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv?$query=select distinct year, month"
  
  try:
    req = requests.get(url, headers=headers , auth=auth)
  except requests.exceptions.RequestException as e:
    print("Unble to query dataset")
    raise SystemExit(e) 
  
  year_months = list(csv.reader(req.text.splitlines()))

  s3_client = get_s3_client(aws_key, aws_sercet)
  
  try:
    all_objects = s3_client.list_objects_v2(Bucket=bucket_name,
                                            Prefix='monthly_counts_raw',
                                            FetchOwner=False,)
  except botocore.exceptions.ClientError as e:
    print("Unable to query aws")
    raise SystemExit(e) 

  if 'Contents' in all_objects.keys():
    file_list = [file['Key'].split('/')[1] for file in all_objects['Contents']]
  else:
    file_list = []
      
  # fs = s3fs.S3FileSystem(key=aws_key, secret=aws_sercet)
  
  monthly_stats = pd.DataFrame() 
  daily_stats = pd.DataFrame()
  
  for ym in year_months[1:]:
    if ym[0]+"-"+ym[1]+".csv" not in file_list:
      url = "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.csv?$limit=50000&Year={}&Month={}".format(ym[0], ym[1])
      try:
        req = requests.get(url, headers=headers , auth=auth)
      except requests.exceptions.RequestException as e:
        print("Unble to query dataset")
        raise SystemExit(e)
      
      data = StringIO(req.text)
      mdf = pd.read_csv(data)
      
      month_sum = mdf.groupby(['year','month','sensor_id', 'sensor_name'])['hourly_counts'].sum().reset_index()
      month_sum['monthly_rank'] = month_sum['hourly_counts'].rank(method='dense', ascending=False)
      monthly_stats = monthly_stats.append(month_sum[month_sum.monthly_rank <= 10].reset_index())
      
      daily_sum = mdf.groupby(['year', 'month', 'mdate', 'day', 'sensor_id', 'sensor_name'])['hourly_counts'].sum().reset_index()
      daily_sum['daily_rank'] = daily_sum.groupby(['mdate'])['hourly_counts'].rank(method='dense', ascending=False)
      daily_stats = daily_stats.append(daily_sum[daily_sum.daily_rank <= 10].reset_index())
      
      csv_buffer = StringIO()
      mdf.to_csv(csv_buffer, index = False)
      res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="monthly_counts_raw/{}-{}.csv".format(ym[0], ym[1]))
    # break
  
  if daily_stats.empty:
    print("Nothing to Update")
  else:
    # fetch location data
    loc_url = "https://data.melbourne.vic.gov.au/resource/h57g-5234.csv"
    try:
      req = requests.get(loc_url, headers=headers , auth=auth)
    except requests.exceptions.RequestException as e:
      print("Unble to query dataset")
      raise SystemExit(e)
    
    data = StringIO(req.text)
    ldf = pd.read_csv(data)
    ldf = ldf.rename(columns={"sensor_name": "sensor_name_short"})
    
    # joining stats with locations and concat with s3 version
    monthly_stats = pd.merge(monthly_stats, ldf , on='sensor_id', how='left')
    
    try:
      obj = s3_client.get_object(Bucket=bucket_name, Key='stats/monthly_counts.csv')
      
      s3msdf = pd.read_csv(io.BytesIO(obj['Body'].read()) , index_col = False)
      monthly_stats = s3msdf.append(monthly_stats, ignore_index = True, sort = False)
      
      csv_buffer = StringIO()
      monthly_stats.to_csv(csv_buffer, index = False)
      res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="stats/monthly_counts.csv")
      
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchKey':
        csv_buffer = StringIO()
        monthly_stats.to_csv(csv_buffer, index = False)
        res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="stats/monthly_counts.csv")
      else:
        print("Unable to query aws")
        raise SystemExit(e)
    
    daily_stats = pd.merge(daily_stats, ldf, on='sensor_id', how='left')
    
    try:
      obj = s3_client.get_object(Bucket=bucket_name, Key='stats/daily_counts.csv')
      
      s3dsdf = pd.read_csv(io.BytesIO(obj['Body'].read()) , index_col = False)
      daily_stats = s3dsdf.append(daily_stats, ignore_index = True, sort = False)
      
      csv_buffer = StringIO()
      daily_stats.to_csv(csv_buffer, index = False)
      res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="stats/daily_counts.csv")

    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchKey':
        csv_buffer = StringIO()
        daily_stats.to_csv(csv_buffer, index = False)
        res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="stats/daily_counts.csv")

      else:
        print("Unable to query aws")
        raise SystemExit(e)

    
    csv_buffer = StringIO()
    ldf.to_csv(csv_buffer, index = False)
    res = s3_client.put_object(Body=csv_buffer.getvalue(), Bucket = bucket_name, Key="locations/sensor_locations.csv")


def main():
  # Please paste Credentials here
  aws_key =
  aws_sercet = 
  sodas_key = 
  sodas_key_secret= 
  # bucket_name =
  bucket_name =

  s3_client = get_s3_client(aws_key, aws_sercet)

  bucket_status = create_ped_bucket(s3_client, bucket_name)

  if bucket_status:
    ingest_data(bucket_name, aws_key, aws_sercet, sodas_key, sodas_key_secret)


if __name__ == "__main__":
    main()