terraform {
  required_providers {
    grafana = {
      source = "grafana/grafana"
    }
  }
}

provider "grafana" {
  url  = "http://localhost:3000/"
  auth = "admin:admin"
}

# Got the type by running:
# curl -s -H "Authorization: Basic YWRtaW46YWRtaW4=" http://localhost:3000/api/datasources |jq

resource "grafana_data_source" "redis-enterprise-cloud" {
  type       = "redis-datasource"
  name       = "RedisEnterpriseCloud"
  url        = "redis-14445.c18475.us-west-2-mz.ec2.cloud.rlrcp.com:14445"
  is_default = true
}

# Grab dashboard
# Need to be sure and export by clicking on the share item - totally messed up

resource "grafana_dashboard" "fraud_scoring" {
  config_json = file("grafana-dashboard.json")
}
