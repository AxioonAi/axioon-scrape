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
yesterday = date.today() - timedelta(days=15)

input = requests.get(f"{os.environ['API_IP']}/scrape/youtube")

input = input.json()

input = input["youtube"]

channel_names = [item["youtube"] for item in input]

channel_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "dateFilter": yesterday,
    "details": True,
    "proxySettings": {
        "useApifyProxy": True
    },
    "start_urls": [{"url": f"https://www.youtube.com/{channel_name}"} for channel_name in channel_names]
}

run = client.actor("TyjYgGDGcTNVmil8z").call(run_input=run_input)

json_array = []
posts_set = set()
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
    
    for item in json_array:
        if item["url"]:
            posts_set.add(item["url"])
        for channel_name, channel_id in zip(channel_names, channel_ids):
            if item["channelShortName"] in channel_name:
                item["channel_id"] = channel_id
                
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)
    posts_array = list(posts_set)
    posts_str = json.dumps(posts_array, indent=4, ensure_ascii=False)

with open("Apify/Results/Youtube/Youtube_Videos.json", "w", encoding="utf-8") as f:
    f.write(json_str)

with open("Apify/Results/Youtube/Youtube_Videos_Urls.json", "w", encoding="utf-8") as f:
    f.write(posts_str)
    
upload_file(f"Apify/Results/Youtube/Youtube_Videos.json", "axioon", f"Apify/YouTube/Videos/YouTube_Videos_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/youtube/video", json={"records": f"Apify/YouTube/Videos/YouTube_Videos_{timestamp}.json"})