#!/usr/bin/python3

from redistimeseries.client import Client as RedisTimeSeries
import time
import datetime
import json
import boto3
import random

stream_name = "demo-stream"

# Kinesis setup
kinesis = boto3.client("kinesis")

response = kinesis.describe_stream(StreamName=stream_name)
shard_id = response['StreamDescription']['Shards'][0]['ShardId']

pre_shard_it = kinesis.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]

# Redis setup
redis = RedisTimeSeries(host='localhost', port=6379, db=1)
key = 'fraud-ts'


while 1==1:
    out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
    for record in out['Records']:
        data = json.loads(record['Data'])
        trans_date_trans_time = data["trans_date_trans_time"]
        date = datetime.datetime.strptime(trans_date_trans_time, "%Y-%m-%d %H:%M:%S")
        timestamp = int(datetime.datetime.timestamp(date))
        merchant = data["merchant"]
        category = data["category"]
        fraud_score = float(data["is_fraud"])
        redis.add(key,timestamp,fraud_score,retention_msecs=30000,labels={'merchant': merchant,'category': category})

        print(timestamp)

    shard_it = out["NextShardIterator"]
    time.sleep(1.0)
