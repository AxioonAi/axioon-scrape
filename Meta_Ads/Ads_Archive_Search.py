from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
import requests
import logging
import boto3
import json
import os

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 "bucket"

    :param file_name: File to upload
    :param "bucket": "bucket" to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3', aws_access_key_id="AKIA6MOM3OQOF7HA5AOG", aws_secret_access_key="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")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

now = datetime.now()
now_in_days = now.strftime("%Y-%m-%d")
timestamp = datetime.timestamp(now)

search_limit = date.today() - timedelta(days=30)
search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")
search_limit = datetime.strftime(search_limit, "%Y-%m-%d")

input = requests.get("http://172.20.10.2:3333/scrape/name")
input = input.json()

input = input["list"]

input_names = [name["social_name"] for name in input]

input_facebook_names = [name["facebook"] for name in input]


input_ids = [name["id"] for name in input]


search_amount = [
    {"name": f"{name["social_name"]}", "bylines": f"{name["name"]}, {name['social_name']}, {name["facebook"]}", "ad_delivery_date_min": f"{search_limit}"} for name in input
    # {"name": "Mauro Mendes, Mauro Mendes Ferreira", "bylines": "Mauro Mendes, Mauro Mendes Ferreira", "ad_delivery_date_min": "2022-09-01"}
]

search_queries = {"ad_reached_countries": "BR", "search_terms": "", "ad_delivery_date_min": "", "bylines": "", "ad_type": "POLITICAL_AND_ISSUE_ADS", "fields": "ad_creation_time,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,bylines,page_name,currency,spend,impressions,delivery_by_region,demographic_distribution","limit": 5000, "access_token": "EAACxJFtlwx0BOzhZB2ncAYDjgwtgRdVwzkVVUD9ZAE4v46EhwBgajFChBeZBDMpmYeUg7mkAD6G5UC6ovaynnpW5GSDRz8pN7Mecu7uKnZB4RkBrS8Evuj3MSpwZCJZAZAXtsrOjw1sFOHjAZCYYorHDeFsZB7mrBcXucQFsUHzpodBTm7igBohdzSHk7ubIx4mZB6l4kSUOsuJOEqZBNYscitK"}

current_version = "v18.0"

search_url = f"https://graph.facebook.com/{current_version}/ads_archive?"

result = []

for item in search_amount:
    item["name"] = item["name"].replace(" ", "%20")
    item["bylines"] = item["bylines"].replace(" ", "%20")
    search_queries["search_terms"] = item["name"]
    search_queries["bylines"] = item["bylines"]
    search_queries["ad_delivery_date_min"] = item["ad_delivery_date_min"]

    r = requests.get(f"{search_url}ad_reached_countries={search_queries['ad_reached_countries']}&search_terms={search_queries['search_terms']}&search_type=KEYWORD_EXACT_PHRASE&ad_delivery_date_min={search_queries['ad_delivery_date_min']}&bylines={search_queries['bylines']}&ad_type={search_queries['ad_type']}&fields={search_queries['fields']}&limit={search_queries['limit']}&access_token={search_queries['access_token']}")

    file_name = item["name"].replace("%20","-")
    
    json_array = r.content.decode("utf-8")
    
    json_array = json.loads(json_array)

    result.append(json_array)
    
    for item in result:
        for individual in item["data"]:
            for input_name, input_facebook_name, input_id in zip(input_names, input_facebook_names, input_ids):
                if individual["page_name"].lower() == input_name.lower() or individual["page_name"].lower() == input_facebook_name.lower():
                    item["Meta_id"] = input_id
               
    
result_str = json.dumps(result, ensure_ascii=False, indent=4)
    
print("result_str: ", result_str)

with open(f"/Results/Meta_Ads_Results_{timestamp}.json", "w") as f:
    f.write(result_str)

upload_file(f"/Results/Meta_Ads_Results_{timestamp}.json", "nightapp", f"Meta_Ads/Meta_Ads_Results_{timestamp}.json")