#!/usr/bin/python3
import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import time
import boto3
import json
import re
import datetime
import random
from scipy.stats import poisson
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import requests

region = boto3.Session().region_name
kinesis_client = boto3.client('kinesis')
stream_name = "demo-stream"


def generate_metadata():
    millisecond_regex = r'\.\d+'
    timestamp = re.sub(millisecond_regex, '', str(datetime.datetime.now()))
    source = random.choice(['Mobile', 'Web', 'Store'])
    result = [timestamp, 'random_id', source]

    return result

def get_data_payload(test_array):
    return {'data':','.join(map(str, test_array)),
            'metadata': generate_metadata()}

def generate_traffic(X_test):
    print("*** IN generate_traffic")
    while True:
        np.random.shuffle(X_test)
        for example in X_test:
            data_payload = get_data_payload(example)
            invoke_endpoint(data_payload)
            # We invoke the function according to a shifted Poisson distribution
            # to simulate data arriving at random intervals
            time.sleep(poisson.rvs(1, size=1)[0] + np.random.rand())


def invoke_endpoint(payload):
    print ("***** Will put this data in to kinesis datastream ******")
    print(payload)
    print("-------------- Record START ---------------")
    payload_json = json.dumps(payload, indent = 4)
    print(payload_json)

    kinesis_client.put_record(
        StreamName=stream_name,
        Data=payload_json,
        PartitionKey="partitionkey")
    print("-------------- Record END   ---------------")
    print("\n")
    print("\n")

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    source_data = os.path.join(base_dir, "data/creditcard.csv")
    data = pd.read_csv(source_data, delimiter=',')
    data[['Time', 'V1', 'V2', 'V27', 'V28', 'Amount', 'Class']].describe()

    feature_columns = data.columns[:-1]
    label_column = data.columns[-1]

    features = data[feature_columns].values.astype('float32')
    print("**** printing features -START ******")
    print(features)
    print("**** printing features -  END ******")

    labels = (data[label_column].values).astype('float32')
    print("**** printing labels -START ******")
    print(labels)
    print("**** printing labels -  END ******")

    #Requires : pip3 install sklearn
    X_train, X_test, y_train, y_test = train_test_split(
    features, labels, test_size=0.1, random_state=42)


    generate_traffic(X_test)
