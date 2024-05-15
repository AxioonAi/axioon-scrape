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

input = requests.get(f"{os.environ['API_IP']}/scrape/without/tiktok")

input = input.json()

input = input["profiles"]

tiktok_names = [item["tiktok"] for item in input]

tiktok_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "disableCheerioBoost": False,
    "disableEnrichAuthorStats": False,
    "profiles": [tiktok_name for tiktok_name in tiktok_names],
    "resultsPerPage": 1000,
    "scrapeLastNDays": 60,
    "shouldDownloadCovers": False,
    "shouldDownloadSlideshowImages": False,
    "shouldDownloadVideos": False
}
run = client.actor("GdWCkxBtKWOsKjdch").call(run_input=run_input)

json_array = []
json_str = ""
posts_str = ""
posts_set = set()
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
    
    for item in json_array:
        if "webVideoUrl" in item:
            if item["webVideoUrl"]:
                posts_set.add(item["webVideoUrl"])
                for tiktok_name, tiktok_id in zip(tiktok_names, tiktok_ids):
                    if tiktok_name.lower() in item["webVideoUrl"].lower():
                        item["tiktok_id"] = tiktok_id

    json_str = json.dumps(json_array, ensure_ascii=False, indent=4)
    posts_array = list(posts_set)
    posts_str = json.dumps(posts_array, ensure_ascii=False, indent=4)

with open(f"/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Posts.json", "w") as f:
    f.write(json_str)

with open(f"/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Posts_Urls.json", "w") as f:
    f.write(posts_str)

upload_file(f"/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Posts.json", "axioon", f"Apify/TikTok/Posts/TikTok_Posts_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/tiktok", json={"records": f"Apify/TikTok/Posts/TikTok_Posts_{timestamp}.json"})
