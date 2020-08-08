#!/usr/bin/bash

## Test for files in /data/git

localGit="/data/git"
numFiles=$(ls -l ${localGit} | wc -l)
numS3Files=$(aws s3 ls s3://tanmatth-emr/covid19/git/COVID-19/ | wc -l)
git_source="https://github.com/CSSEGISandData/COVID-19.git"

if [ "$numFiles" -eq "1" ]; then
  git clone ${git_source}
else
  git pull
fi

