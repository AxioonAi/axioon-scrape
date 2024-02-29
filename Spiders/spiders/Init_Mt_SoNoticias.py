from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from ..items import articleItem
from scrapy.http import Request

from bs4 import BeautifulSoup
import requests
import logging
import scrapy
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

today = date.today().strftime("%d/%m/%Y")
today = datetime.strptime(today, "%d/%m/%Y")

search_limit = date.today() - timedelta(days=60)
search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")

site_id = "1f225ec9-b50a-412f-b437-30f3889fe077"

# INIT API ROUTE
request = requests.get(f"{os.environ['API_IP']}/scrape/without/news/{site_id}")
search_words = request.json()

with open("/home/scrapeops/axioon-scrape/Spiders/CSS_Selectors/MT/Mt_SoNoticias.json") as f:
    search_terms = json.load(f)
    
class InitMtSonoticiasSpider(scrapy.Spider):
    name = "Init_Mt_SoNoticias"
    allowed_domains = ["sonoticias.com.br"]
    start_urls = ["https://sonoticias.com.br/politica-listagem"]
    
    def parse(self, response):
        for article in response.css(search_terms['article']):
            link = article.css(search_terms['link']).get()
            yield Request(link, callback=self.parse_article, priority=1)
        next_page = response.css(search_terms['next_page']).get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
            
            
    def parse_article(self, response):
        updated = response.css(search_terms['updated']).get()
        updated = datetime.strptime(updated, '%d/%m/%Y %H:%M')
        updated = updated.replace(hour=0, minute=0)
        title = response.css(search_terms['title']).get()
        content = response.css(search_terms['content']).getall()
        content = BeautifulSoup(" ".join(content), "html.parser").text
        content = content.replace("\n", " ")
        if search_limit <= updated <= today:
            found_names = []
            # for paragraph in content:
            for user in search_words['users']:
                if user['social_name'] in content:
                    found_names.append({'name': user['social_name'], 'id': user['id']})
                    item = articleItem(
                        updated=updated,
                        title=title,
                        content=content,
                        link=response.url,
                        users=found_names,
                        site_id=site_id
                    )
                    yield item
                    if item is not None:
                        article_dict = {
                            "updated": item['updated'].strftime("%d/%m/%Y"),
                            "title": item['title'],
                            "content": [item['content']],
                            "link": item['link'],
                            "users": item['users'],
                            "site_id": item['site_id']
                        }
                        file_path = f"/home/scrapeops/axioon-scrape/Spiders/Results/{self.name}_{timestamp}.json"
                        if not os.path.isfile(file_path):
                            with open(file_path, "w") as f:
                                json.dump([], f)

                        with open(file_path, "r") as f:
                            data = json.load(f)

                        data.append(article_dict)

                        with open(file_path, "w") as f:
                            json.dump(data, f, ensure_ascii=False)
                            
                        upload_file(f"/home/scrapeops/axioon-scrape/Spiders/Results/{self.name}_{timestamp}.json", "axioon", f"News/MT/{self.name}_{timestamp}.json")
                        file_name = requests.post(f"{os.environ['API_IP']}/webhook/news", json={"records": f"News/MT/{self.name}_{timestamp}.json"})
                     
        else:
            raise scrapy.exceptions.CloseSpider
        