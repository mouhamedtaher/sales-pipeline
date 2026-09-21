"""Lambda déclenchée par un événement S3 (nouveau fichier dans
sales-pipeline-raw/). Démarre le Glue Job process_sales_data en lui
passant le chemin exact du fichier déposé comme --input_path.
"""
import json
import urllib.parse

import boto3

GLUE_JOB_NAME = "process_sales_data"
OUTPUT_PATH = "s3://aws-ex-med/sales-pipeline-results/"

glue_client = boto3.client("glue")


def lambda_handler(event, context):
    """Point d'entrée Lambda.

    Parameters
    ----------
    event : dict
        Événement S3 (ObjectCreated) contenant le bucket et la clé du
        fichier déposé.
    context : object
        Contexte d'exécution Lambda (non utilisé ici).
    """
    record = event["Records"][0]["s3"]
    bucket_name = record["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(record["object"]["key"])
    input_path = f"s3://{bucket_name}/{object_key}"

    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--input_path": input_path,
            "--output_path": OUTPUT_PATH,
        },
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Glue job started",
                "job_run_id": response["JobRunId"],
                "input_path": input_path,
            }
        ),
    }
