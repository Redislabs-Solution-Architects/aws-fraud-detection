# AWS Fraud Detection

This repo contains utility code for the [Redis Enterprise Fraud Detection Solution Brief.](https://www.google.com) <br>
They are for demonstration purposes and not meant for production.  <br><br>
Make sure that you have installed `redis-py 4.x` for both RedisJSON and RedisTimeSeries as `redistimeseries-py` is deprecated. <br>
You can check the version with the `pip show redis` command.

## `./data/fraud_test_data.csv`
Under the data folder there is sample credit card transaction file. <br>
 This file is only first 100 lines of a [Kaggle Dataset.](https://www.kaggle.com/kartik2112/fraud-detection)

## `./utilities/producer.py`
Producer.py reads the transactions from the `./data/fraud_test_data.csv` and sends them over to a Kinesis stream as JSON document. You can pass an argument to the script like, by default it's 100:
 ```
python3 ./utilities/producer.py 1000
```
This argument tells the script to process number of lines from the last processed line. The last processed line is stored on the `./data/last_line.txt`

You can create a Kinesis stream via AWS CLI like:
```
aws kinesis create-stream --stream-name demo-stream --shard-count 1
```

## `./utilities/consumer_json.py`
Consumer_json.py reads the transaction from the Kinesis stream and sinks it to the RedisJSON database. RedisJSON is the primary database of the solution.

## `./utilities/consumer_ts.py`
Consumer_ts.py reads the transaction from the Kinesis stream and sinks it to the RedisTimeSeries database. This TS database is mainly to be used for real-time dashboards/reports.

## `./utilities/query_ts.py`
Query_ts.py shown an example on how to consume data from RedisTimeSeries database. Please refer to the [documentation](https://redis-py.readthedocs.io/en/stable/redismodules.html#redistimeseries-commands) for all the commands and their parameters.
```python
get_mrange(
    from_time = "-", # minimum possible timestamp
    to_time = "+", # maximum possible timestamp
    aggregation_type = "count", # optional aggregation type.
    bucket_size_msec = 1000, # time bucket for aggregation in milliseconds
    filters = ["category=home"] # filter to match the time-series labels.
)
```
