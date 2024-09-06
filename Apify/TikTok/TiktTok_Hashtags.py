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
yesterday = date.today() - timedelta(days=1)

input = requests.get(f"{os.environ['API_IP']}/scrape/hashtag")

input = input.json()

tiktok_hashtags = [item["hashtag"] for item in input]

tiktok_ids = [item["id"] for item in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "hashtags": tiktok_hashtags,
    "resultsPerPage": 20,
    "shouldDownloadVideos": False,
    "shouldDownloadCovers": False,
    "shouldDownloadSubtitles": False,
    "shouldDownloadSlideshowImages": False,
}

run = client.actor("f1ZeP0K58iwlqG2pY").call(run_input=run_input)

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
                for tiktok_hashtag, tiktok_id in zip(tiktok_hashtags, tiktok_ids):
                    if tiktok_hashtag in [x["name"] for x in item["hashtags"]]:
                        item["hashtag_id"] = tiktok_id
    
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)
    posts_array = list(posts_set)
    posts_str = json.dumps(posts_array, ensure_ascii=False, indent=4)
    
with open("/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Hashtags.json", "w", encoding="utf-8") as f:
    f.write(json_str)
    
with open(f"/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Hashtags_Urls.json", "w", encoding="utf-8") as f:
    f.write(posts_str)
    
upload_file("/home/scrapeops/axioon-scrape/Apify/Results/TikTok/TikTok_Hashtags.json", "axioon", f"Apify/TikTok/Hashtags/TikTok_Hashtags_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/tiktok/hashtag/mentions", json={"records": f"Apify/TikTok/Hashtags/TikTok_Hashtags_{timestamp}.json"})
