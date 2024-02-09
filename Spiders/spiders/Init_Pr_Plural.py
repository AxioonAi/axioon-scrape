from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from ..items import articleItem
from scrapy.http import Request
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import logging
import locale
import scrapy
import boto3
import json
import os

load_dotenv()

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), region_name="us-east-1")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

now = datetime.now()
timestamp = datetime.timestamp(now)

today = date.today().strftime("%d/%m/%Y")
today = datetime.strptime(today, "%d/%m/%Y")

search_limit = date.today() - timedelta(days=60)
search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")

request = requests.get(f"{os.getenv('API_IP')}/scrape/news/069f1600-bc0a-49b7-95a8-5832e98a0d7d")
search_words = request.json()

# INIT API ROUTE
with open("/home/scrapeops/Axioon/Spiders/CSS_Selectors/PR/Pr_Plural.json") as f:
    search_terms = json.load(f)

main_url = "https://www.plural.jor.br/noticias/"

class InitPrPluralSpider(scrapy.Spider):
    name = "Init_Pr_Plural"
    allowed_domains = ["plural.jor.br"]
    start_urls = ["https://www.plural.jor.br/noticias/"]
    
    def parse(self, response):
        for article in response.css(search_terms['article']):
            link = article.css(search_terms['link']).get()
            yield Request(link, callback=self.parse_article, priority=1)
        next_page = response.css(search_terms['next_page']).get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)
        else:
            print("NÃO TEM NEXT BUTTON")
            
    def parse_article(self, response):
        updated = response.css(search_terms['updated']).get()
        updated = updated.replace(" de ", " ").strip()
        updated = datetime.strptime(updated, "%d %B %Y").strftime("%d/%m/%Y")
        updated = datetime.strptime(updated, "%d/%m/%Y")
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
                        users=found_names
                    )
                    yield item
                    if item is not None:
                        article_dict = {
                            "updated": item['updated'].strftime("%d/%m/%Y"),
                            "title": item['title'],
                            "content": [item['content']],
                            "link": item['link'],
                            "users": item['users']
                        }
                        file_path = f"Spiders/Results/{self.name}_{timestamp}.json"
                        if not os.path.isfile(file_path):
                            with open(file_path, "w") as f:
                                json.dump([], f)

                        with open(file_path, "r") as f:
                            data = json.load(f)

                        data.append(article_dict)

                        with open(file_path, "w") as f:
                            json.dump(data, f, ensure_ascii=False)
                            
                        upload_file(f"Spiders/Results/{self.name}_{timestamp}.json", "axioon", f"News/PR/{self.name}_{timestamp}.json")
                        file_name = requests.post(f"{os.getenv('API_IP')}/webhook/news", json={"records": f"News/PR/{self.name}_{timestamp}.json"})
                     
        else:
            raise scrapy.exceptions.CloseSpider