# AWS Fraud Detection

This repo contains utility code for the [Redis Enterprise Fraud Detection Solution Brief.](https://www.google.com) <br>
They are for demonstration purposes and not meant for production.  <br><br>
Make sure that you have installed `redis-py 4.x` for both RedisJSON and RedisTimeSeries as `redistimeseries-py` is deprecated. <br>
You can check the version with the `pip show redis` command.

## Pre-requisites
Prior to running this application, please ensure following pre-requisites are installed and configured.

- [Docker](https://www.docker.com/products/docker-desktop)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Terraform](https://www.terraform.io/downloads.html)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## Setup:

First enable a Python virtual environment.

```
python3 -m venv aws-fraud-detection-demo.venv
source aws-fraud-detection-demo.venv/bin/activate
pip install -r requirements.txt
```

Next, run the Producer.
```
python3 ./utilities/producer.py
```
This will start producing events into an AWS kinesis stream.

Let's consume these events by running a consumer, in another terminal or SSH window.

```
python3 ./utilities/consumer_json.py

```

Open another window to consume the data and persist it in to Redis Timeseries.

```
python3 ./utilities/consumer_ts.py
```

Start the docker containers.

```
docker-compose up
```

Next, edit the `terraform\grafana.tf` to reflect Redis Enterprise Cloud endpoint ( Hostname: Port). Search for `grafana_data_source` section and update the Redis endpoint.

Using terraform, install Redis data source plugin in the Grafana docker container and also setup Grafana dashboards.

```
cd terraform
terraform init
terraform apply
```

Once the containers are up and running, fire up a browser and point it to :

[Dashboard Link](http://localhost:3000)

Login as admin/admin. Feel free to skip changing the default Grafana password.
![img](docs/images/1-grafana-login.png)

Next, go to the settings and make sure the Data source `RedisEnterpriseCloud` is correctly configured with appropriate password and hit "Save and Test" button.
![img](docs/images/2-grafana-data-sources.png)

![img](docs/images/3-grafana-redis-datasource.png)

![img](docs/images/4-grafana-redis-datasource-config.png)

![img](docs/images/5-grafana-redis-datasource-test.png)

Now go to Dahsboards and load up the only dashboard that is already configured using terraform. The dashboard should be displaying the TimeSeries visualizations.

![img](docs/images/6-grafana-dashboard.png)

![img](docs/images/7-grafana-dashboard.png)

## Cleanup:

```
terraform destroy
docker-compose down
```

# More details on utilities
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
