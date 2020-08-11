#!/usr/bin/env python3

import boto3
import csv
import datetime
import io
import json
import logging
import numpy as np
import os
import pandas as pd
import re
import requests

from git import Repo
from datetime import datetime, timedelta

## Variables

### Source
git_s3_store = "tanmatth-emr"
git_s3_store_key = "/covid19/git/COVID-19/"

### Temp storage on container
temp_location = "/data/git/COVID-19/"

### DynamoDB
item_table = "covid19_download_status"

### Destination
dest_s3_bucket = "tanmatth-emr"
dest_s3_basekey = "covid-19/jhu/"
dest_s3_daily_file = "daily.csv"

### Data type folders
data_folder = "csse_covid_19_data/"
world_reports = "csse_covid_19_daily_reports/"
us_only_reports = "csse_covid_19_daily_reports_us/"
timeseries_reports = "csse_covid_19_time_series/"

### S3 Objects
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

### dates
world_start_date = datetime.strptime("2020-03-22",'%Y-%m-%d')
us_start_date = datetime.strptime("2020-04-12",'%Y-%m-%d')
end_date = datetime.today() - 1 * timedelta(days=1)

def download_dir(prefix, local, bucket, client=s3_client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - client: initialized s3 client object
    """
    keys = []
    dirs = []
    next_token = ''
    base_kwargs = {
      'Bucket':bucket,
      'Prefix':prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != '':
            kwargs.update({'ContinuationToken': next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get('Contents')
        for i in contents:
            k = i.get('Key')
            if k[-1] != '/':
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get('NextContinuationToken')
    for d in dirs:
        dest_pathname = os.path.join(local, d)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    for k in keys:
        dest_pathname = os.path.join(local, k)
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
        client.download_file(bucket, k, dest_pathname)
    
    return keys

def getCsvFile(month,day,year):
  jhu_file = month + "-" + day + "-" + year + ".csv"
  return jhu_file

def get_dynamo_check(item_id_prefix,day,month,year):
  dynamodb = boto3.resource("dynamodb", region_name='us-east-2')
  table = dynamodb.Table('covid19_csse_download_status')
  datetime_value = year + '-' + month + '-' + day

  item = table.get_item(
    Key={
      "itemid":  item_id_prefix+'-'+datetime_value,
      "datetime": datetime_value
    }
  )

  return item

def put_dynamo_check(item_id_prefix,day,month,year,data_category,git_status):
  dynamodb = boto3.resource("dynamodb", region_name='us-east-2')
  table = dynamodb.Table('covid19_csse_download_status')
  datetime_value = year + '-' + month + '-' + day
  filename_value = month + '-' + day + '-' + year + '.csv'
  now = datetime.now()
  now_date = now.strftime("%Y-%m-%d")

  table.put_item(
    Item={
      'itemid': item_id_prefix+'-'+datetime_value,
      'datetime': datetime_value,   
      'filename': filename_value,
      'data_category': data_category,
      'gitstatus': git_status
    }
  )

def search_dynamo(item_id_prefix,day,month,year):
  dynamodb = boto3.resource("dynamodb", region_name='us-east-2')
  table = dynamodb.Table('covid19_csse_download_status')
  datetime_value = year + '-' + month + '-' + day

  s = table.query(
    KeyConditionExpression=Key('itemid').eq(item_id_prefix+'-'+datetime_value)
  )

  return s.get("Items")

def update_dynamo_check(item_id_prefix,day,month,year,data_category,git_status):
  dynamodb = boto3.resource("dynamodb", region_name='us-east-2')
  table = dynamodb.Table('covid19_csse_download_status')
  datetime_value = year + '-' + month + '-' + day
  now = datetime.now()
  now_date = now.strftime("%Y-%m-%d")

  table.update_item(
    Key={'itemid': item_id_prefix+'-'+datetime_value, 'datetime': datetime_value},
    UpdateExpression="SET gitstatus=:git_status, update_datetime=:update_datetime",
    ExpressionAttributeValues={
        ":gitstatus": git_status,
        ":update_datetime": now_date,
    },
  )


def upload_s3(day,month,year,source,dest_bucket,dest_key):
  s3 = boto3.resource('s3')
  #s3key = dest_key + 'year=' + year + '/month=' + month + '/day=' + day + '/' + daily_file
  s3.meta.client.upload_file(source, dest_bucket, dest_key)

  return_msg = "Uploaded to S3 on s3://" + dest_bucket + '/' + dest_key

  return return_msg

def upload_update(month,day,year,folder,daily_file,item_id_prefix,data_category):

  git_status = repo.git.log("-n", "1", "--pretty=format:%ar", "--", world_folder + daily_file)

  item = get_dynamo_check(item_id_prefix,day,month,year)
  curr_git_status = item.get("Item").get("gitstatus")

  uploady = True
    
  if item.get("Item"):
    if curr_git_status == git_status:
      uploady = False
      print("Will not copy as status same")
    else:
      update_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)
  else:
    put_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)

  if uploady == True:
    source = temp_location + data_folder + world_reports + daily_file
    dest_key = dest_s3_basekey + data_folder + world_reports + 'year=' + year + '/month=' + month + '/day=' + day + '/' + dest_s3_daily_file

    print(upload_s3(day,month,year,source,dest_s3_bucket,dest_key))


### Main code starts here
###################################

# Setting the Git Repo
repo = Repo(temp_location)

# World Daily Reports

curr_date = world_start_date
world_folder = temp_location + data_folder + world_reports
data_category = "world daily reports"
item_id_prefix = "1"

for i in range( (end_date - world_start_date).days + 1 ):
    print(curr_date)
    day = str(curr_date.day).rjust(2, '0')
    month = str(curr_date.month).rjust(2, '0')
    year = str(curr_date.year)

    daily_file = getCsvFile(month,day,year)

    upload_update(month,day,year,world_folder,daily_file,item_id_prefix,data_category)

    #daily_file = getCsvFile(month,day,year)

    #git_status = repo.git.log("-n", "1", "--pretty=format:%ar", "--", world_folder + daily_file)

    #item = get_dynamo_check(item_id_prefix,day,month,year)
    #curr_git_status = item.get("Item").get("gitstatus")

    #uploady = True
    
    #if item.get("Item"):
    #  if curr_git_status == git_status:
    #    uploady = False
    #    print("Will not copy as status same")
    #  else:
    #    update_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)
    #else:
    #  put_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)

    #if uploady == True:
    #  source = temp_location + data_folder + world_reports + daily_file
    #  dest_key = dest_s3_basekey + data_folder + world_reports + 'year=' + year + '/month=' + month + '/day=' + day + '/' + dest_s3_daily_file

    #  print(upload_s3(day,month,year,source,dest_s3_bucket,dest_key))


    # Add 1 day ti curr_date
    curr_date = curr_date + timedelta(days=1)


# US Daily Reports

curr_date = us_start_date
us_folder = temp_location + data_folder + us_only_reports
data_category = "us daily reports"
item_id_prefix = "2"

for r in range( (end_date - us_start_date).days + 1):
  print(curr_date)
  day = str(curr_date.day).rjust(2, '0')
  month = str(curr_date.month).rjust(2, '0')
  year = str(curr_date.year)

  daily_file = getCsvFile(month,day,year)

  upload_update(month,day,year,us_folder,daily_file,item_id_prefix,data_category)

  #git_status = repo.git.log("-n", "1", "--pretty=format:%ar", "--", us_folder + daily_file)

  #if ( get_dynamo_check(item_id_prefix,day,month,year) ):
  #  update_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)
  #else:
  #  put_dynamo_check(item_id_prefix,day,month,year,data_category,git_status)
  
  #source = temp_location + data_folder + us_only_reports + daily_file
  #dest_key = dest_s3_basekey + data_folder + us_only_reports + 'year=' + year + '/month=' + month + '/day=' + day + '/' + dest_s3_daily_file

  #print(upload_s3(day,month,year,source,dest_s3_bucket,dest_key))

  curr_date = curr_date + timedelta(days=1)

# Time series

time_series_list = [time_series_covid19_confirmed_US, time_series_covid19_confirmed_global, time_series_covid19_deaths_US, time_series_covid19_deaths_global, time_series_covid19_recovered_global]

source = temp_location + data_folder + timeseries_reports
series_file = "series.csv"

s3 = boto3.resource('s3')

for series in time_series_list:
  source_file = source + series + ".csv"
  dest_key = dest_s3_basekey + data_folder + timeseries_reports + series + "/" + series_file

  s3.meta.client.upload_file(source_file, dest_s3_bucket, dest_key)
