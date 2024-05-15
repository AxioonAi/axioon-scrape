from datetime import datetime, date, timedelta
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
yesterday = date.today() - timedelta(days=1)

input = requests.get(f"{os.environ['API_IP']}/scrape/facebook")

input = input.json()

input = input["facebook"]

facebook_names = [item["facebook"] for item in input]

facebook_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = { 
    "startUrls": [
        { "url": f"https://www.facebook.com/{facebook_name}/" } for facebook_name in facebook_names
        ],
    "onlyPostsNewerThan": yesterday
    }

run = client.actor("4Hv5RhChiaDk6iwad").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
    
    for item in json_array:
        for facebook_name, facebook_id in zip(facebook_names, facebook_ids):
            if item["facebookUrl"].lower() == f"https://www.facebook.com/{facebook_name}/".lower():
                item["facebook_id"] = facebook_id
    
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)
    
with open("/home/scrapeops/axioon-scrape/Apify/Results/Facebook/Facebook_Pages.json", "w") as f:
    f.write(json_str)
    
upload_file(f"/home/scrapeops/axioon-scrape/Apify/Results/Facebook/Facebook_Pages.json", "axioon", f"Apify/Facebook/Pages/Facebook_Pages_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/facebook/profile", json={"records": f"Apify/Facebook/Pages/Facebook_Pages_{timestamp}.json"})
