import logging
import json
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.session import get_session

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# connect to OpenSearch domain
try:
    openSearchHost = "search-photos-5fs2fd32xismuc3coqoqwkbi3q.us-east-1.es.amazonaws.com"  # copy domain url here
    region = os.environ["AWS_REGION"]
    # Pulls credentials for open search connectiono
    session = get_session()
    credentials = session.get_credentials()
    # Authenticaton for OpenSearch requests
    awsauth = AWS4Auth(
        credentials.access_key, \
        credentials.secret_key, \
        region, \
        "es", \
        session_token=credentials.token
    )
    # Connect o OpenSearch
    openSearchClient = OpenSearch(
        hosts=[{"host": openSearchHost, "port": 443}], \
        http_auth=awsauth, \
        use_ssl=True, \
        verify_certs=True, \
        connection_class=RequestsHttpConnection
    )
except Exception as e:
    logger.error(f"Failed to initialize OpenSearch client: {e}")
    openSearchClient = None


def main(event, context):
    # Log event and context
    logger.info("## EVENT RECEIVED ##")
    logger.info(json.dumps(event))
    logger.info("## CONTEXT INFO ##")
    logger.info(f"Function Name: {context.function_name}")
    logger.info(f"Remaining Time (ms): {context.get_remaining_time_in_millis()}")
    logger.info(f"Request ID: {context.aws_request_id}")

    if openSearchClient == None:
        return
        {
            "statusCode": 500,
            "body": "Failed to initialize OpenSearch client."
        }
    # Pull info form event
    records = event.get("Records", {})
    bucket = records[0]["s3"]["bucket"]["name"]
    photo = records[0]["s3"]["object"]["key"]

    # Get metadata from photo
    s3Client = boto3.client("s3")
    s3Response = s3Client.head_object(Bucket=bucket, Key=photo)
    photoMetadata = s3Response.get("Metadata", {})
    A1 = json.loads(photoMetadata.get("customlabels", "[]"))
    # log metadata
    logger.info("Photo metadata labels")
    logger.info(A1)

    # Get labels from Rekognition
    rekognitionClient = boto3.client("rekognition")
    rekResponse = rekognitionClient.detect_labels(
        Image={
            'S3Object': {
                "Bucket": bucket,
                "Name": photo
            }
        }
    )
    rekLabels = [label["Name"] for label in rekResponse.get("Labels", "[]")]
    A1.extend(rekLabels)

    # log labels
    logger.info("Rekognition Labels:")
    logger.info(json.dumps(rekResponse, indent=4))

    index = \
        {
            "objectKey": photo,
            "bucket": bucket,
            "createdTimestamp": s3Response.get("LastModified").isoformat(),
            "labels": A1
        }
    logger.info("Indexing photo:")
    logger.info(json.dumps(index, indent=4))
    osResponse = openSearchClient.index(
        index="photos", \
        id=photo, \
        body=index)
    if osResponse["_shards"]["successful"] == 1:
        logger.info("Succesfully indexed photo:")
        logger.info(json.dumps(osResponse, indent=4))
        return
        {
            "statusCode": 200,
            "body": "Photo indexed successfully."
        }
    else:
        logger.info("Failed to index photo:")
        logger.info(json.dumps(osResponse, indent=4))
        return
        {
            "statusCode": 500,
            "body": "Failed to index photo."
        }