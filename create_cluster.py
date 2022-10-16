import pandas as pd
import boto3
import json
import configparser
import logging
import time

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

def attach_policy(iam):
    logging.info('Attaching Policy to IAM Role')

    response = iam.attach_role_policy(
        RoleName=dwh_iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']
    logging.info(f'=== IAM ROLE ===\nResponse: {response}')
    assert(response == 200), f'The HTTP status code is {response}'

def create_cluster(redshift, role_arn):
    logging.info('Creating Redshift Cluster')
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=dwh_cluster_type,
            NodeType=dwh_node_type,
            NumberOfNodes=int(dwh_num_nodes),

            #Identifiers & Credentials
            DBName=dwh_db,
            ClusterIdentifier=dwh_cluster_identifier,
            MasterUsername=dwh_db_user,
            MasterUserPassword=dwh_db_password,
            
            #Roles (for s3 access)
            IamRoles=[role_arn]  
        )
    except Exception as e:
        print(e)

def pretty_redshift_props(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def update_config_file(parameter_to_replace, value):
    config_file = 'dwh.cfg'

    with open(config_file, 'r') as file:
        data = file.readlines():
    
    for i, line in enumerate(data):
        if line.strip().startswith(parameter_to_replace):
            data[i] = f'{parameter_to_replace} = {value}\n'
    
    with open(config_file, 'w') as file:
        file.writelines(data)

def open_tcp_port(ec2, cluster_props):
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        default_security_group = list(vpc.security_groups.all())[0]
        print(default_security_group)
        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )
    except Exception as e:
        print(e)

def main():
    ec2, s3, iam, redshift = create_resources()
    dwh_role = create_iam_role(iam)
    attach_policy(iam)
    role_arn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']
    create_cluster(redshift, role_arn)
    cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    prettified_cluster_props = pretty_redshift_props(cluster_props)
    print(prettified_cluster_props)
    
    while cluster_props['ClusterStatus'] != 'available':
        print(f'Cluster Status: {cluster_props.items()["ClusterStatus"]}')
        print('Retrying in 5s')
        time.sleep(5)
        cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    # Só executar linhas abaixo depois de testar a parte superior
    dwh_endpoint = cluster_props['Endpoint']['Address']
    dwh_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", dwh_endpoint)
    print("DWH_ROLE_ARN :: ", dwh_role_arn)
    update_config_file('DWH_ENDPOINT', dwh_endpoint)
    update_config_file('DWH_ROLE_ARN', dwh_role_arn)

    open_tcp_port(ec2, cluster_props)
if __name__ == '__main__':
    main()
