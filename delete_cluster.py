import configparser
import logging
import time
import boto3

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

    return iam, redshift

def main():
    iam, redshift = create_resources()
    redshift.delete_cluster( ClusterIdentifier=dwh_cluster_identifier,  SkipFinalClusterSnapshot=True)


    while True:
        try:
            cluster_props = redshift.describe_clusters(ClusterIdentifier=dwh_cluster_identifier)['Clusters'][0]
            logging.info(f'Cluster status: {cluster_props["ClusterStatus"]}')
            logging.info('Rechecking in 5 seconds')
            time.sleep(5)
        except:
            logging.info('Cluster was deleted')
            break
    
    logging.info('Detaching role policy:')
    response = iam.detach_role_policy(RoleName=dwh_iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    logging.info(response)
    logging.info('Deleting role')
    response = iam.delete_role(RoleName=dwh_iam_role_name)
    logging = response

if __name__ == '__main__':
  main()
