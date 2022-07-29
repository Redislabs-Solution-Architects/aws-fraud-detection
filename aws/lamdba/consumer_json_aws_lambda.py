from __future__ import print_function
import base64
import json
import redis
import boto3
import time
import os
import random
import datetime
import re

import boto3
import base64
import time
import configparser
from redis.commands.json.path import Path

stream_name = "demo-stream"
sg_runtime = boto3.client('runtime.sagemaker',region_name='us-west-2')
ENDPOINT_NAME = "random-cut-forest-endpoint"
#redis_client = redis.Redis(host='redis-15381.c20502.us-east-1-mz.ec2.cloud.rlrcp.com', port=15381, password="t4Ye29t1ZpPCfoVhIV73uRGHEd8Gvmhc", db=0)
config = configparser.ConfigParser()
config.read("lambda_configs.properties")
print(config.sections())
REDIS_HOST = "localhost"
REDIS_PORT = "6379"
REDIS_PWD = ""
if ("REDIS" in config):
    print ("redis configs exist")
    REDIS_HOST = config['REDIS']['REDIS_HOST']
    REDIS_PORT = config['REDIS']['REDIS_PORT']
    REDIS_PWD = config['REDIS']['REDIS_PWD']
    print(REDIS_HOST)
    print(REDIS_PORT)
    print(REDIS_PWD)

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PWD, db=0)


def lambda_handler(event, context):
    print("***** 3 Redis Demo : Fraud Detection : Lambda Function - START ********")
    redis_client = redis.Redis(host='redis-15381.c20502.us-east-1-mz.ec2.cloud.rlrcp.com', port=15381, password="t4Ye29t1ZpPCfoVhIV73uRGHEd8Gvmhc", db=0)
    print (redis_client)
    for record in event['Records']:
        print ("**Found data in Kinesis Datastream. Processing - START")
        payload=base64.b64decode(record["kinesis"]["data"])
        payload_dict = json.loads(payload)
        print(payload_dict)

        metadata = payload_dict["metadata"]
        print("****metadata=")
        print(metadata)
        data_payload = payload_dict["data"]
        print("****data_payload=")
        print(data_payload)

        persistTransactionalData(payload_dict)
        output = makeInferences(data_payload)
        persistMLScores(output)
        print ("**Found data in Kinesis Datastream. Processing - END")
        return output

    print("***** Redis Demo : Fraud Detection : Lambda Function - END ********")

def persistTransactionalData(payload_dict):
    print("** persistTransactionalData - START")
    now = datetime.datetime.now() # current date and time
    trans_date_trans_time = now.strftime("%Y/%m/%d-%H:%M:%S")
    key = "fraud:" + trans_date_trans_time

    redis_client.json().set(key, Path.root_path(), payload_dict)
    result = redis_client.json().get(key)
    print(result)
    print("** persistTransactionalData - END")

def makeInferences(data_payload):
    print("** makeInferences - START")
    output = {}
    output["anomaly_detector"] = get_anomaly_prediction(data_payload)
    output["fraud_classifier"] = get_fraud_prediction(data_payload)
    print(output)
    print("** makeInferences - END")
    return output

def persistMLScores(output):
    print("** persistMLScores - START")
    print(output)

    fraud_score = round(random.uniform(0.1, 1.0), 10)
    print ("**** fraud_score = %2.2f" % (fraud_score))

    key = 'fraud-ts'
    now = datetime.datetime.now() # current date and time
    timestamp = int(round(now.timestamp()))
    print(timestamp)

    redis_client.ts().add(key,timestamp,fraud_score,duplicate_policy='last')
    if (fraud_score >= 0.7 ):
        redis_client.ts().add("fraudulent_ts","*",fraud_score,duplicate_policy='last', labels={'type': "fraud_score"})
        print("*** adding to fraudulent series with current timestamp")
    else:
        redis_client.ts().add("non-fraudulent_ts","*",fraud_score,duplicate_policy='last', labels={'type': "fraud_score"})
        print("*** adding to non-fraudulent series with current timestamp")

    print("** persistMLScores - END")


def get_anomaly_prediction(data):
    sagemaker_endpoint_name = 'random-cut-forest-endpoint'
    sagemaker_runtime = boto3.client('sagemaker-runtime')
    response = sagemaker_runtime.invoke_endpoint(EndpointName=sagemaker_endpoint_name, ContentType='text/csv',
                                                 Body=data)
    print("****response from get_anomaly_prediction=")
    print(response)
    # Extract anomaly score from the endpoint response
    anomaly_score = json.loads(response['Body'].read().decode())["scores"][0]["score"]
    print("anomaly score: {}".format(anomaly_score))
    return {"score": anomaly_score}


def get_fraud_prediction(data, threshold=0.5):
    sagemaker_endpoint_name = 'fraud-detection-endpoint'
    sagemaker_runtime = boto3.client('sagemaker-runtime')
    response = sagemaker_runtime.invoke_endpoint(EndpointName=sagemaker_endpoint_name, ContentType='text/csv',
                                                 Body=data)
    print("****response from get_fraud_prediction=")
    print(response)
    pred_proba = json.loads(response['Body'].read().decode())
    print(pred_proba)
    prediction = 0 if pred_proba < threshold else 1
    # Note: XGBoost returns a float as a prediction, a linear learner would require different handling.
    print("classification pred_proba: {}, prediction: {}".format(pred_proba, prediction))

    return {"pred_proba": pred_proba, "prediction": prediction}
