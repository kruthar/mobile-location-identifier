# Mobile Location Identifier
This project is an interview coding assignment for X-Mode, developed by Andy Kruth.

## Build
This is a simple Maven setup. Build the jar:

```
mvn clean package
```

The functional jar should be availabe at `target/mobilelocationidentifier-1.0.jar`.

## Run
The application acceptes the following parameters:

```
mobile    - (Required) Path to a parquet file or directory of parquet files containing the mobile point data.
locations - (Required) Path to a csv file or directory of csv files containing your target location data.
output    - Path to an output location (otherwise a data sample will be printed in spark output).
outformat - Format for output data (defaults to csv)
start     - Starting Timestamp in milliseconds
end       - Ending Timestamp in milliseconds
```

The application jar can be run with spark-submit. An example command is provided in the `run.sh` script and also below:

```
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
```