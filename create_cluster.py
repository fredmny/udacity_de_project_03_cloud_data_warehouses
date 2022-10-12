from venv import create
import pandas as pd
import boto3
import json
import configparser
import logging

logging.basicConfig(level=logging.DEBUG)

config = configparser.ConfigParser()
config.read('dwh.cfg')

# Set variables from config file 
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

def create_resources():

    ec2 = boto3.resource(
        'ec2',
        region_name='us-west-2',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    s3 = boto3.resource(
        's3',
        region_name='us-west-2',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    redshift = boto3.client(
        'redshift',
        region_name='us-west-2',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    return ec2, s3, iam, redshift

def create_iam_role(iam):
    logging.info('Creating IAM Role')
    try:
        dwh_role = iam.create_role(
            Path='/',
            RoleName=dwh_iam_role_name,
            Description='Allows Redshift clusters to call AWS services on your behalf',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [
                    {'Action': 'sts:AssumeRole', 'Effect': 'Allow', 'Principal':{
                        'Service': 'redshift.amazonaws.com'
                    }}
                ],
                'Version': '2012-10-17'}
            )
        )
        return dwh_role

    except Exception as e:
        print(e)

def attach_policy(iam, dwh_role):
    print('Attaching Policy to IAM Role')

    response = iam.attach_role_policy(
        RoleName=dwh_iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']
    logging.info(f'Response: {response}')

def main():
    ec2, s3, iam, redshift = create_resources()
    dwh_role = create_iam_role(iam)
    attach_policy(iam, dwh_role)

if __name__ == '__main__':
    main()
