#!/usr/bin/bash

## Test for files in /data/git

gitFolder="COVID-19"
localGit="/data/git"
localGitFolder="${localGit}/${gitFolder}/"
s3bucket="tanmatth-emr"
s3key="/covid-19/jhu/COVID-19/"
numFiles=$(ls -l ${localGit} | wc -l)
numS3Files=$(aws s3 ls s3://tanmatth-emr/covid19/git/COVID-19/ | wc -l)
git_source="https://github.com/CSSEGISandData/COVID-19.git"

cd ${localGit}

if [ "$numFiles" -eq "1" ]; then
  git clone ${git_source}
else
  git pull
fi

aws s3 cp --recursive ${localGitFolder} s3://${s3bucket}${s3key}