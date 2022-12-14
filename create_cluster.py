from cmath import log
import pandas as pd
import boto3
import json
import configparser
import logging
import time

logging.basicConfig(level=logging.INFO)

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
    """Generates AWS resources using the boto3 package

    Returns:
        Objects to operate AWS resources:
        - EC2
        - S3
        - IAM
        - Redshift
    """    
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
    """Create IAM role

    Args:
        iam: IAM resource generated through boto3

    Returns:
        DWH role
    """    
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
    """Attach policy to IAM role

    Args:
        iam: IAM resource generated through boto3
    """    
    logging.info('Attaching Policy to IAM Role')

    response = iam.attach_role_policy(
        RoleName=dwh_iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']
    logging.info(f'=== IAM ROLE ===\nResponse: {response}')
    assert(response == 200), f'The HTTP status code is {response}'

def create_cluster(redshift, role_arn):
    """Create Redshift cluster

    Args:
        redshift: Redshift resource generated through boto3
        role_arn: ARN of IAM role
    """
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
        logging.info('Cluster created')
        logging.info(response)
    except Exception as e:
        print(e)

def pretty_redshift_props(props):
    """Prettify Redshift cluster data, using pandas

    Args:
        props: Redshift cluster data, as returned by the `describe_cluster` parameter

    Returns:
        Pandas dataframe with prettified cluster data
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def update_config_file(parameter_to_replace, value):
    """Update dwh.cfg configuration file

    Args:
        parameter_to_replace: The parameter that should be replaced
        value: The value of the parameter
    """
    config_file = 'dwh.cfg'

    with open(config_file, 'r') as file:
        data = file.readlines()
    
    for i, line in enumerate(data):
        if line.strip().startswith(parameter_to_replace):
            data[i] = f'{parameter_to_replace}={value}\n'
    
    with open(config_file, 'w') as file:
        file.writelines(data)

def open_tcp_port(ec2, cluster_props):
    """Open TCP port on EC2 instance

    Args:
        ec2: EC2 resource generated through boto3
        cluster_props: Redshift cluster data, as returned by the `describe_cluster` parameter
    """
    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        default_security_group = list(vpc.security_groups.all())[0]
        logging.info(f'Default security group:\n{default_security_group}')
        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(dwh_port),
            ToPort=int(dwh_port)
        )
        logging.info('TCP port opened successfully')
    except Exception as e:
        print(e)

def main():
    """Create Redshift cluster and all prior steps to be able to do so. Also save dwh endpoint and dwh role arn to config file dwh.cfg
    """
    # Instantiate resources
    ec2, s3, iam, redshift = create_resources()
    # Create IAM role and attach policy
    dwh_role = create_iam_role(iam)
    attach_policy(iam)
    role_arn = iam.get_role(RoleName=dwh_iam_role_name)['Role']['Arn']
    # Create cluster
    create_cluster(redshift, role_arn)
    cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
    prettified_cluster_props = pretty_redshift_props(cluster_props)
    logging.info(prettified_cluster_props)
    logging.info(cluster_props)
    cluster_status = cluster_props['ClusterStatus']
    while cluster_status != 'available':
        logging.info(f'Cluster Status: {cluster_props["ClusterStatus"]}')
        logging.info('Retrying in 5s')
        time.sleep(5)
        cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
        cluster_status =cluster_props['ClusterStatus']
    logging.info(f'Cluster Status: {cluster_props["ClusterStatus"]}')
    # Updaate config file
    dwh_endpoint = cluster_props['Endpoint']['Address']
    dwh_role_arn = cluster_props['IamRoles'][0]['IamRoleArn']
    logging.info("DWH_ENDPOINT :: ", dwh_endpoint)
    logging.info("DWH_ROLE_ARN :: ", dwh_role_arn)
    update_config_file('HOST', dwh_endpoint)
    update_config_file('ARN', dwh_role_arn) # IF ERROR WITH THIS PART, TRY PUTTING dwh_role_arn BETWEEN QUOTES
    logging.info('Config file updated')
    # Open TCP port
    logging.info('Opening TCP port')
    open_tcp_port(ec2, cluster_props)

if __name__ == '__main__':
    main()
