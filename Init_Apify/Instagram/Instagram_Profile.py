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
last_week = date.today() - timedelta(days=7)

# INIT API ROUTE
# input = requests.get(f"{os.environ['API_IP']}/scrape/without/instagram")

# input = input.json()

input = {
  "profiles": [
    {
      "instagram": "amiltonfilhooficial",
      "id": "bef3421d-8c74-4758-8aaf-6e898e1f26b1"
    }
  ]
}

input = input["profiles"]

instagram_names = [item["instagram"] for item in input]

instagram_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "directUrls": [f"https://www.instagram.com/{instagram_name}" for instagram_name in instagram_names],
    "resultsType": "details",
    "resultsLimit": 20,
    "addParentData": False,
    "searchType": "hashtag",
    "searchLimit": 1,
}

run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
    
    for item in json_array:
        for instagram_name, instagram_id in zip(instagram_names, instagram_ids):
            if item["username"].lower() == instagram_name.lower():
                item["instagram_id"] = instagram_id
    
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)

with open("Init_Apify/Results/Instagram/Instagram_Profiles.json", "w") as f:
    f.write(json_str)
    
upload_file("Init_Apify/Results/Instagram/Instagram_Profiles.json", "axioon", f"Apify/Instagram/Profiles/Instagram_Profiles_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/instagram/profile", json={"records": f"Apify/Instagram/Profiles/Instagram_Profiles_{timestamp}.json"})
