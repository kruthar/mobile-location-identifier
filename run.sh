#!/usr/bin/env bash

spark-submit \
  --class io.xmode.kruthar.mobilelocationidentifier.LocationMatcher \
  --master local[*] \
  target/mobilelocationidentifier-1.0.jar \
  mobile=/Users/kruthar/projects/xmode/large.parquet \
  locations=/Users/kruthar/projects/xmode/top100.csv \
  output=/Users/kruthar/projects/xmode/output.csv \
  outformat=parquet \
  start=1605657600000 \
  end=1605744000000
