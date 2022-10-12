import pandas as pd
import boto3
import json
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

key = config.get('AWS','KEY')
secret = config.get('AWS','SECRET')

dwh_cluster_type = config.get("AWS","DWH_CLUSTER_TYPE")
dwh_num_nodes = config.get("AWS","DWH_NUM_NODES")
dwh_node_type = config.get("AWS","DWH_NODE_TYPE")

dwh_cluster_identifier = config.get("AWS","DWH_CLUSTER_IDENTIFIER")
dwh_db = config.get("CLUSTER","DB_NAME")
dwh_db_user = config.get("CLUSTER","DB_USER")
dwh_db_password = config.get("CLUSTER","DB_PASSWORD")
dwh_port = config.get("CLUSTER","DB_PORT")

dwh_iam_role_name = config.get("AWS", "DWH_IAM_ROLE_NAME")


