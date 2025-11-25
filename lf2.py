import boto3
import json
import requests

def search_handle(event, context, intent_name):
    query = event.get("inputTranscript", "")

    slots = event.get("sessionState", {}).get("intent", {}).get("slots", {})
    keywords = []

    for slot_name, slot in slots.items():
        if slot and 'value' in slot and 'interpretedValue' in slot['value']:
            keywords.append(slot['value']['interpretedValue'])
    
    if not keywords and query:
        keywords = query.lower().split()
    
    if not keywords:
        results = []
    else:
        #opensearch query
        should_clauses = [
            {"terms": {"labels.keyword": keywords}}
        ]
        os_query = {
            "size": 100,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            }
        }

        url = f"https://" #TO DO ADD OPENSEARCH DOMAIN HERE
        resp = requests.post(url, json=os_query)
        body = resp.json()
        hits = body.gets("hits", {}).get("hits", [])

        results = [
            {
                "objectKey": h["_source"]["objectKey"],
                "bucket": h["_source"]["bucket"],
                "labels": h["_source"]["labels"],
                "createdTimestamp": h["_source"]["createdTimestamp"]
            }
            for h in hits
        ]
        return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"results": results})
    }


def lambda_handler(event, context):
    intent_name = event["SessionState"]["intent"]["name"]
    try:
        if intent_name == "search-photos":
            return search_handle(event, context, intent_name)
    except Exception as e:
        # Return proper Lex error format
        return {
            "sessionState": {
                "dialogAction": {"type": "Close"},
                "intent": {"name": intent_name, "state": "Failed"}
            },
            "messages": [
                {"contentType": "PlainText", "content": f"Oops! Something went wrong: {str(e)}"}
            ]
        }
