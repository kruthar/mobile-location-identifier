#!/usr/bin/env bash

spark-submit \
  --class io.xmode.kruthar.mobilelocationidentifier.LocationMatcher \
  --master local[*] \
  target/mobilelocationidentifier-1.0-jar-with-dependencies.jar