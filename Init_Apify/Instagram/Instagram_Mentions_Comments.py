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

with open("Init_Apify/Results/Instagram/Instagram_Mentions_Urls.json") as f:
    input = json.load(f)

client = ApifyClient(os.environ['APIFY_KEY'])

run_input = {
    "directUrls": input,
    "resultsLimit": 1000
}

run = client.actor("SbK00X0JYCPblD2wp").call(run_input=run_input)

json_array = []
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    json_data = json.dumps(item, ensure_ascii=False)
    json_array.append(json.loads(json_data))
                
    json_str = json.dumps(json_array, indent=4, ensure_ascii=False)

with open("Init_Apify/Results/Instagram/Instagram_Mentions_Comments.json", "w") as f:
    f.write(json_str)
    
upload_file("Init_Apify/Results/Instagram/Instagram_Mentions_Comments.json", "axioon", f"Apify/Instagram/Mentions_Comments/Instagram_Mentions_Comments_{timestamp}.json")

file_name = requests.post(f"{os.environ['API_IP']}/webhook/instagram/mentions/comments", json={"records": f"Apify/Instagram/Mentions_Comments/Instagram_Mentions_Comments_{timestamp}.json"})
