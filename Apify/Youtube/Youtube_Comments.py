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

with open("/home/scrapeops/axioon-scrape/Apify/Results/Youtube/Youtube_Videos_Urls.json", "r") as f:
    input = json.load(f)

input = [{"url": url} for url in input]

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "maxComments": 50,
    "startUrls": input
}

run = client.actor("p7UMdpQnjKmmpR21D").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
                
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)

with open("/home/scrapeops/axioon-scrape/Apify/Results/Youtube/Youtube_Comments.json", "w", encoding="utf-8") as f:
    f.write(json_str)
    
upload_file("/home/scrapeops/axioon-scrape/Apify/Results/Youtube/Youtube_Comments.json", "axioon", f"Apify/YouTube/Comments/YouTube_Comments_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/youtube/comments", json={"records": f"Apify/YouTube/Comments/YouTube_Comments_{timestamp}.json"})
