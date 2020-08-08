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


## Variables

### Source
git_source = "https://github.com/CSSEGISandData/COVID-19.git"
git_s3_store = "tanmatth-emr"
git_s3_store_key = "/covid19/git/COVID-19/"

### Temp storage on container
temp_location = "/data/git/"

### Destination
dest_s3_bucket = "tanmatth-emr"
dest_s3_basekey = "/covid-19/jhu/"

### Data type folders
world_reports = "csse_covid_19_daily_reports"
us_only_reports = "csse_covid_19_daily_reports_us"
timeseries_reports = "csse_covid_19_time_series"

### S3 Objects
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

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

### Main code starts here
###################################

k = download_dir(git_s3_store_key, temp_location, git_s3_store, s3_client)
keys_str = str(k)
## Check local repo diff with remote repo
repo = Repo(local_git_folder)
origin = repo.remotes.origin
print("REMOTE:" + origin.url)
origin.fetch()
diff_txt = repo.git.diff("--stat", "HEAD",  "origin/master", local_git_folder)
diff_txt.count("/csse_covid_19_daily_reports/")

#jhu_changes = re.findall("\/csse_covid_19_daily_reports\/.*.csv", diff_txt)
  
## Pull latest changes from remote
origin.pull()

## Upload pulled data back into S3 git store
s3_updates = upload_files(local_git_folder,source_bucket,source_key)

## Upload new data into JHU S3 store
