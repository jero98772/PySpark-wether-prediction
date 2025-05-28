import boto3
import os
import json

# --------------- Configuration ---------------

REGION = os.getenv("AWS_REGION", "us-east-1")
EMR_RELEASE = "emr-6.15.0"
LOG_URI = "s3://your-bucket-name/logs/"
EC2_KEY_NAME = "your-ec2-keypair-name"
SUBNET_ID = "subnet-xxxxxxxx"  # Must be in same region
S3_BUCKET = "your-bucket-name"

INSTANCE_TYPE = "m5.xlarge"
INSTANCE_COUNT = 3

# --------------- EMR Setup ---------------

def create_emr_cluster():
    emr = boto3.client("emr", region_name=REGION)

    response = emr.run_job_flow(
        Name="WeatherDataPipelineCluster",
        ReleaseLabel=EMR_RELEASE,
        Applications=[{"Name": "Spark"}],
        LogUri=LOG_URI,
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master node",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": INSTANCE_COUNT - 1,
                },
            ],
            "Ec2KeyName": EC2_KEY_NAME,
            "KeepJobFlowAliveWhenNoSteps": True,
            "Ec2SubnetId": SUBNET_ID,
        },
        JobFlowRole="EMR_EC2_DefaultRole",       # Assumes default roles exist
        ServiceRole="EMR_DefaultRole",
        VisibleToAllUsers=True,
        EbsRootVolumeSize=32,
        Tags=[
            {"Key": "Project", "Value": "WeatherPipeline"},
        ],
    )

    cluster_id = response["JobFlowId"]
    print(f"✅ EMR cluster created with ID: {cluster_id}")
    return cluster_id

# --------------- Main Execution ---------------

if __name__ == "__main__":
    create_emr_cluster()
