from botocore.exceptions import ClientError
from apify_client import ApifyClient

from datetime import datetime
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

input = requests.get(f"{os.environ['API_IP']}/scrape/tiktok")

input = input.json()

input = input["tiktok"]

tiktok_names = [item["tiktok"] for item in input]

comments_input = []

with open(f"Apify/Results/TikTok/TikTok_Posts_Urls.json") as f:
    comments_input = json.load(f)

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "postURLs": comments_input,
    "commentsPerPost": 1000,
    "maxRepliesPerComment": 0,
}
run = client.actor("BDec00yAmCm1QbMEI").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))

    json_str = json.dumps(json_array, ensure_ascii=False, indent=4)

with open(f"Apify/Results/TikTok/TikTok_Comments.json", "w") as f:
    f.write(json_str)
    
upload_file(f"Apify/Results/TikTok/TikTok_Comments.json", "axioon", f"Apify/TikTok/Comments/TikTok_Comments_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/tiktok/comments", json={"records": f"Apify/TikTok/Comments/TikTok_Comments_{timestamp}.json"})
