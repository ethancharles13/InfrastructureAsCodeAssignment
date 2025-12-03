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
    logger.info("## EVENT RECEIVED ##")
    logger.info(json.dumps(event))

    if openSearchClient is None:
        logger.error("OpenSearch client is not initialized.")
        return {
            "statusCode": 500,
            "body": "Failed to initialize OpenSearch client."
        }

    records = event.get("Records", [])
    if not records:
        logger.error("No Records in event")
        return {
            "statusCode": 400,
            "body": "No Records in event."
        }

    from urllib.parse import unquote_plus
    bucket = records[0]["s3"]["bucket"]["name"]
    photo = unquote_plus(records[0]["s3"]["object"]["key"])

    # Get metadata
    s3Client = boto3.client("s3")
    s3Response = s3Client.head_object(Bucket=bucket, Key=photo)
    photoMetadata = s3Response.get("Metadata", {})
    raw_custom = photoMetadata.get("customlabels")

    if raw_custom:
        A1 = [x.strip() for x in raw_custom.split(",") if x.strip()]
    else:
        A1 = []

    logger.info("Photo metadata labels: %s", A1)

    # Rekognition
    rekognitionClient = boto3.client("rekognition")
    rekResponse = rekognitionClient.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": photo}}
    )
    rekLabels = [label["Name"] for label in rekResponse.get("Labels", [])]
    A1.extend(rekLabels)

    logger.info("Rekognition Labels:")
    logger.info(json.dumps(rekResponse, indent=4))

    index_doc = {
        "objectKey": photo,
        "bucket": bucket,
        "createdTimestamp": s3Response.get("LastModified").isoformat(),
        "labels": A1
    }

    logger.info("Indexing photo document:")
    logger.info(json.dumps(index_doc, indent=4))

    try:
        osResponse = openSearchClient.index(
            index="photos",
            id=photo,
            body=index_doc
        )
        logger.info("OpenSearch response: %s", json.dumps(osResponse, indent=4))
    except Exception as e:
        logger.error("Error indexing document into OpenSearch: %s", e, exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Exception while indexing photo: {str(e)}"
        }

    if osResponse.get("_shards", {}).get("successful") == 1:
        logger.info("Successfully indexed photo")
        return {
            "statusCode": 200,
            "body": "Photo indexed successfully."
        }
    else:
        logger.error("Failed to index photo")
        return {
            "statusCode": 500,
            "body": "Failed to index photo."
        }