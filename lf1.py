import logging
import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main(event, context):
    # Log event and context
    logger.info("## EVENT RECEIVED ##")
    logger.info(json.dumps(event))
    logger.info("## CONTEXT INFO ##")
    logger.info(f"Function Name: {context.function_name}")
    logger.info(f"Remaining Time (ms): {context.get_remaining_time_in_millis()}")
    logger.info(f"Request ID: {context.aws_request_id}")

    # Pull info form event
    records = event.get("Records", {})
    bucket = records[0]["s3"]["bucket"]["name"]
    photo = records[0]["s3"]["object"]["key"]

    # Get metadata from photo
    s3Client = boto3.client("s3")
    s3Response = s3Client.head_object(Bucket=bucket, Key=photo)
    photoMetadata = s3Response.get("Metadata", {})
    A1 = json.loads(photoMetadata.get("customlabels", []))
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
    rekLabels = [label["Name"] for label in rekResponse.get("Labels", [])]
    A1.extend(rekLabels)

    # log labels
    logger.info("Rekognition Labels:")
    logger.info(json.dumps(rekResponse, indent=4))

    # Send OpenSearch index
    index = \
        {
            "objectKey": photo,
            "bucket": bucket,
            "createdTimestamp": s3Response.get("LastModified").isoformat(),
            "labels": A1
        }
    # TODO: Send index to OpenSearch
    return
    {
        "statusCode": 200,
        "body": "Photo indexed successfully."
    }