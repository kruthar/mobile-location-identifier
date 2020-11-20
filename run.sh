#!/usr/bin/env bash

spark-submit \
  --class io.xmode.kruthar.mobilelocationidentifier.LocationMatcher \
  --master local[*] \
  target/mobilelocationidentifier-1.0.jar \
  mobile=/Users/kruthar/projects/xmode/tiny.parquet \
  locations=/Users/kruthar/projects/xmode/top100.csv
