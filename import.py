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
dest_s3_basekey = "/covid-19/jhu/"

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

def put_dynamo_check(day,month,year,git_status):
  dynamodb = boto3.resource("dynamodb", region_name='us-east-2')
  table = dynamodb.Table('covid19_download_status')
  datetime_value = year + '-' + month + '-' + day
  filename_value = month + '-' + day + '-' + year + '.csv'

  table.put_item(
    Item={
      'datetime': datetime_value,
      'filename': filename_value,
      'gitstatus': git_status
    }
  )

def upload_s3(day,month,year,jhu_file):
  daily_file = "daily.csv"
  s3 = boto3.resource('s3')
  s3key = rootkey + 'year=' + year + '/month=' + month + '/day=' + day + '/' + daily_file
  s3.meta.client.upload_file(jhu_folder + jhu_file, bucket, s3key)

  return_msg = "Uploaded to S3 on s3://" + bucket + '/' + s3key

  return return_msg

### Main code starts here
###################################

#k = download_dir(git_s3_store_key, temp_location, git_s3_store, s3_client)
#keys_str = str(k)
## Check local repo diff with remote repo
#repo = Repo(local_git_folder)
#origin = repo.remotes.origin
#print("REMOTE:" + origin.url)
#origin.fetch()
#diff_txt = repo.git.diff("--stat", "HEAD",  "origin/master", local_git_folder)
#diff_txt.count("/csse_covid_19_daily_reports/")

#jhu_changes = re.findall("\/csse_covid_19_daily_reports\/.*.csv", diff_txt)
  
## Pull latest changes from remote
#origin.pull()

## Upload pulled data back into S3 git store
#s3_updates = upload_files(local_git_folder,source_bucket,source_key)

## Upload new data into JHU S3 store



#status = repo.git.log("-n", "1", "--pretty=format:%ar", "--", "/data/git/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/08-07-2020.csv")

# World Daily Reports

curr_date = world_start_date
world_folder = temp_location + data_folder + world_reports

repo = Repo(temp_location)

for i in range( (end_date - world_start_date).days ):
    print(curr_date)
    day = str(curr_date.day).rjust(2, '0')
    month = str(curr_date.month).rjust(2, '0')
    year = str(curr_date.year)

    daily_file = getCsvFile(month,day,year)

    git_status = repo.git.log("-n", "1", "--pretty=format:%ar", "--", world_folder + daily_file)
    
    put_dynamo_check(day,month,year,git_status)
    
    curr_date = curr_date + timedelta(days=1)