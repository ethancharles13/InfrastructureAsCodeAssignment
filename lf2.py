import json
import os
import logging

import botocore.session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import urllib.request
import urllib.error

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
SERVICE = "es"  # OpenSearch/ES service name for SigV4

# From CloudFormation:
#   OS_ENDPOINT: !GetAtt PhotosDomain.DomainEndpoint  (no protocol, just host)
#   OS_INDEX: "photos"
OS_ENDPOINT = os.environ["OS_ENDPOINT"]          # e.g. "search-photos-xxxx.us-east-1.es.amazonaws.com"
OS_INDEX = os.environ.get("OS_INDEX", "photos")


# --- Low-level signed HTTP client to OpenSearch --- #

_session = botocore.session.get_session()
_credentials = _session.get_credentials()


def signed_opensearch_request(path: str, method: str = "GET", body: dict | None = None) -> tuple[int, str]:
    """
    Send a signed HTTP request to the OpenSearch domain using SigV4 + urllib.
    Returns (status_code, body_text), even for non-2xx responses.
    """
    host = OS_ENDPOINT.replace("https://", "").replace("http://", "").rstrip("/")
    base_url = f"https://{host}"
    url = base_url + path

    if body is None:
        body_bytes = b""
    else:
        body_str = json.dumps(body)
        body_bytes = body_str.encode("utf-8")

    headers = {
        "Host": host,
        "Content-Type": "application/json",
    }

    aws_request = AWSRequest(method=method, url=url, data=body_bytes, headers=headers)
    SigV4Auth(_credentials, SERVICE, REGION).add_auth(aws_request)
    prepared = aws_request.prepare()

    req = urllib.request.Request(
        url=prepared.url,
        data=body_bytes if method in ("POST", "PUT") else None,
        headers=dict(prepared.headers),
        method=method,
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp_body = resp.read().decode("utf-8")
            status = resp.getcode()
    except urllib.error.HTTPError as e:
        # Non-2xx status: capture status and body instead of raising
        status = e.code
        resp_body = e.read().decode("utf-8", errors="ignore")
        logger.error("OpenSearch HTTPError %s for %s: %s", status, url, resp_body)
    except Exception as e:
        logger.exception("Error calling OpenSearch at %s", url)
        # Re-raise so lambda_handler turns this into a 500
        raise

    return status, resp_body


def search_photos(keywords: list[str]) -> list[dict]:
    """
    Run a terms query on labels.keyword with the given keywords.
    Returns a list of photo dicts: {objectKey, bucket, labels, createdTimestamp}.
    """
    if not keywords:
        return []

    query = {
        "size": 100,
        "query": {
            "bool": {
                "should": [
                    {"terms": {"labels.keyword": keywords}}
                ],
                "minimum_should_match": 1
            }
        }
    }

    path = f"/{OS_INDEX}/_search"
    status, body_text = signed_opensearch_request(path, method="POST", body=query)

    if status != 200:
        logger.error("OpenSearch returned non-200: %s, body=%s", status, body_text)
        raise RuntimeError(f"OpenSearch search failed with status {status}")
    if status == 404:
        logger.warning("OpenSearch index '%s' not found when searching, returning empty results", OS_INDEX)
        return []

    payload = json.loads(body_text or "{}")
    hits = payload.get("hits", {}).get("hits", [])

    results = []
    for h in hits:
        src = h.get("_source", {})
        results.append(
            {
                "objectKey": src.get("objectKey"),
                "bucket": src.get("bucket"),
                "labels": src.get("labels", []),
                "createdTimestamp": src.get("createdTimestamp"),
            }
        )
    return results


def main(event, context):
    """
    Read ?q=... from API Gateway proxy event and split into simple keywords.
    """
    q_params = event.get("queryStringParameters") or {}
    raw = (q_params.get("q") or "").strip()
    if not raw:
        return []

    # Split on commas and whitespace; strip empties
    tokens: list[str] = []
    for part in raw.split(","):
        for tok in part.strip().split():
            if tok:
                tokens.append(tok)
    return tokens


def main(event, context):
    """
    Lambda proxy integration handler for GET /search?q=...
    """
    logger.info("Event: %s", json.dumps(event))

    keywords = _parse_keywords_from_event(event)
    if not keywords:
        body = {"results": []}
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Content-Type": "application/json",
            },
            "body": json.dumps(body),
        }

    try:
        results = search_photos(keywords)
        body = {"results": results}
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Content-Type": "application/json",
            },
            "body": json.dumps(body),
        }
    except Exception as e:
        logger.exception("Search failed")
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Content-Type": "application/json",
            },
            "body": json.dumps(
                {
                    "message": "Search failed",
                    "error": str(e),
                }
            ),
        }
