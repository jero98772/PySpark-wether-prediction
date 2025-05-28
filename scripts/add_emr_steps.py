import boto3
import os

# --------------- Configuration ---------------

REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = "your-bucket-name"
ETL_SCRIPT = f"s3://{S3_BUCKET}/scripts/etl.py"
ANALYSIS_SCRIPT = f"s3://{S3_BUCKET}/scripts/analyze.py"

# Replace with your cluster ID (or pass it dynamically if needed)
CLUSTER_ID = "j-XXXXXXXXXXXXX"

# --------------- Step Definitions ---------------

def get_spark_step(name, script_path):
    """
    Returns a configured EMR Spark step.
    """
    return {
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                script_path
            ]
        }
    }

# --------------- Add Steps to Cluster ---------------

def add_steps():
    emr = boto3.client("emr", region_name=REGION)

    steps = [
        get_spark_step("ETL: Raw to Trusted", ETL_SCRIPT),
        get_spark_step("Analysis: Trusted to Refined", ANALYSIS_SCRIPT)
    ]

    response = emr.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=steps
    )

    step_ids = response["StepIds"]
    print("✅ Added steps to cluster:")
    for sid in step_ids:
        print(f"  - Step ID: {sid}")

if __name__ == "__main__":
    add_steps()
