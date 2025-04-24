from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from apify_client import ApifyClient

import requests
import logging
import boto3
import json
import os

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name="us-east-1")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

now = datetime.now()
timestamp = datetime.timestamp(now)
last_week = date.today() - timedelta(days=1)

input = requests.get(f"{os.environ['API_IP']}/scrape/youtube")

input = input.json()

input = input["youtube"]

channel_names = [item["youtube"] for item in input]

channel_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "maxResultStreams": 0,
    "maxResults": 1,
    "maxResultsShorts": 0,
    "startUrls": [{"url": f"https://www.youtube.com/{channel_name}"} for channel_name in channel_names]
}

run = client.actor("67Q6fmd8iedTVcCwY").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
    
    for item in json_array:
        if "inputChannelUrl" in item and item["inputChannelUrl"] is not None:
            for channel_name, channel_id in zip(channel_names, channel_ids):
                if item["inputChannelUrl"] in channel_name:
                    item["channel_id"] = channel_id
                
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)

with open(f"/home/scrapeops/axioon-scrape/Apify/Results/Youtube/Youtube_Channels_{timestamp}.json", "w", encoding="utf-8") as f:
    f.write(json_str)
    
upload_file(f"/home/scrapeops/axioon-scrape/Apify/Results/Youtube/Youtube_Channels_{timestamp}.json", "axioon", f"Apify/YouTube/Channels/YouTube_Channels_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/youtube/channel", json={"records": f"Apify/YouTube/Channels/YouTube_Channels_{timestamp}.json"})
