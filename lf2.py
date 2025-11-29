import json
import os
import uuid

import boto3
import requests
from requests_aws4auth import AWS4Auth

# --- Config ---

REGION = "us-east-1"
SERVICE = "es"

OS_HOST = {os.environ['OS_ENDPOINT']}
OS_INDEX = "photos"
OS_URL = f"https://{OS_HOST}/{OS_INDEX}/_search"

# Lex V2 config
LEX_BOT_ID = os.environ.get("LEX_BOT_ID", "errorID")
LEX_BOT_ALIAS_ID = os.environ.get("LEX_BOT_ALIAS_ID", "errorAlias")
LEX_LOCALE_ID = "en_US"

# --- AWS clients and auth setup ---

session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    SERVICE,
    session_token=credentials.token
)

lex_runtime = boto3.client("lexv2-runtime", region_name=REGION)


# --- Helpers ---

def get_keywords_from_lex(text: str, session_id: str) -> list[str]:
    """
    Send text to Lex, return list of keyword strings extracted from intent slots.
    """
    response = lex_runtime.recognize_text(
        botId=LEX_BOT_ID,
        botAliasId=LEX_BOT_ALIAS_ID,
        localeId=LEX_LOCALE_ID,
        sessionId=session_id,
        text=text,
    )

    # Lex v2 response structure
    session_state = response.get("sessionState", {}) or {}
    intent = session_state.get("intent", {}) or {}
    slots = intent.get("slots", {}) or {}

    keywords = []

    # Collect all slot interpretedValues as keywords
    for slot in slots.items():
        if not slot:
            continue
        value = slot.get("value", {})
        interpreted = value.get("interpretedValue")
        if interpreted:
            keywords.append(interpreted)

    # Fallback: if Lex didn’t give us slots, use simple tokenization
    if not keywords:
        keywords = text.lower().split()

    return keywords


def search_opensearch(keywords: list[str]) -> list[dict]:
    """
    Query OpenSearch for given keyword list, return list of photo docs. Change
    """
    if not keywords:
        return []

    os_query = {
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

    resp = requests.post(OS_URL, auth=awsauth, json=os_query)
    resp.raise_for_status()
    body = resp.json()

    hits = body.get("hits", {}).get("hits", [])

    results = [
        {
            "objectKey": h["_source"].get("objectKey"),
            "bucket": h["_source"].get("bucket"),
            "labels": h["_source"].get("labels"),
            "createdTimestamp": h["_source"].get("createdTimestamp"),
        }
        for h in hits
    ]
    return results


# --- Lambda entrypoint ---

def lambda_handler(event, context):
    """
    LF2:
    - Reads query param q from API Gateway
    - Sends q to Lex for NLU
    - Uses Lex slots as keywords to search OpenSearch
    - Returns JSON array of photos
    """

    query_params = event.get("queryStringParameters") or {}
    q = query_params.get("q") or query_params.get("query") or ""

    if not q:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing query parameter 'q'"})
        }

    try:
        session_id = str(uuid.uuid4())

        keywords = get_keywords_from_lex(q, session_id)
        
        keywords = [k.title() for k in keywords]

        results = search_opensearch(keywords)

        # Return results to API Gateway caller
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json","Access-Control-Allow-Origin": "*",         # or your S3 website origin
        "Access-Control-Allow-Headers": "Content-Type,x-api-key",
        "Access-Control-Allow-Methods": "GET,OPTIONS"
        },
            "body": json.dumps({
                "query": q,
                "keywords": keywords,
                "results": results
            })
        }

    except Exception as e:
        # Log in CloudWatch if you like
        print(f"Error: {e}")

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }
#test