from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from scrapy.signals import spider_closed
from scrapy.http import Request
from ..items import articleItem
from bs4 import BeautifulSoup
import requests
import logging
import scrapy
import locale
import boto3
import json
import os

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3',
                            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            region_name="us-east-1")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
    except ClientError as e:
        logging.error(e)
        return False
    return True

now = datetime.now()
timestamp = datetime.timestamp(now)

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

today = datetime.strptime(date.today().strftime("%d/%m/%Y"), "%d/%m/%Y")
search_limit = datetime.strptime((date.today() - timedelta(days=1)).strftime("%d/%m/%Y"), "%d/%m/%Y")

site_id = "029a3c5d-b0f9-42a8-9a96-eaf94216e46b"

# Get search words from API
request = requests.get(f"{os.environ['API_IP']}/scrape/news/{site_id}")
search_words = request.json()

with open("/home/scrapeops/axioon-scrape/Spiders/CSS_Selectors/GO/Go_DiarioDaManha.json") as f:
    search_terms = json.load(f)

class GoDiarioDaManhaSpider(scrapy.Spider):
    name = "Go_DiarioDaManha"
    allowed_domains = ["dm.com.br"]
    start_urls = ["https://www.dm.com.br/ajax/noticiasCategory?offset=0&categoryId=49&amount=20"]
    INCREMENT = 0

    # New attributes for better control
    data = []  # Store all valid articles
    article_count = 0
    found_old_articles = False
    MAX_ARTICLES = 100

    def parse(self, response):
        if self.article_count >= self.MAX_ARTICLES or self.found_old_articles:
            self.crawler.engine.close_spider(self, "Reached article limit or found older articles")
            return

        articles_in_timeframe = 0

        for article in response.css(search_terms['article']):
            if self.article_count >= self.MAX_ARTICLES:
                break

            link = article.css(search_terms['link']).get()
            if link:
                articles_in_timeframe += 1
                yield Request(f"https://dm.com.br{link}", callback=self.parse_article, priority=1)

        # Stop if no new articles found
        if articles_in_timeframe == 0:
            self.found_old_articles = True
            self.crawler.engine.close_spider(self, "No new articles found")
            return

        self.INCREMENT += 20
        next_page = f"https://www.dm.com.br/ajax/noticiasCategory?offset={self.INCREMENT}&categoryId=49&amount=20"
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def parse_article(self, response):
        if self.article_count >= self.MAX_ARTICLES:
            return

        try:
            updated = response.css(search_terms['updated']).get()
            updated = updated.split(",")[1]
            updated = updated.split("-")[0].strip()
            updated = updated.replace("de ", "").strip()
            updated = datetime.strptime(updated, "%d  %B  %Y").strftime("%d/%m/%Y")
            updated = datetime.strptime(updated, "%d/%m/%Y")

            title = response.css(search_terms['title']).get()
            content = response.css(search_terms['content']).getall()
            content = BeautifulSoup(" ".join(content), "html.parser").text
            content = content.replace("\n", " ")

            if search_limit <= updated <= today:
                found_names = []
                for user in search_words['users']:
                    if user['social_name'] in content:
                        found_names.append({'name': user['social_name'], 'id': user['id']})

                if found_names:  # Only process if we found matching names
                    item = articleItem(
                        updated=updated,
                        title=title,
                        content=content,
                        link=response.url,
                        users=found_names,
                        site_id=site_id
                    )
                    self.data.append(item)
                    self.article_count += 1
                    yield item
            else:
                self.found_old_articles = True
                self.crawler.engine.close_spider(self, "Found older articles")

        except Exception as e:
            logging.error(f"Error processing article {response.url}: {str(e)}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(GoDiarioDaManhaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=spider_closed)
        return spider

    def spider_closed(self, spider):
        """Handle all data upload when spider closes"""
        if not self.data:
            return

        # Prepare the data
        articles_data = []
        for item in self.data:
            article_dict = {
                "updated": item['updated'].strftime("%d/%m/%Y"),
                "title": item['title'],
                "content": item['content'],
                "link": item['link'],
                "users": item['users'],
                "site_id": item['site_id']
            }
            articles_data.append(article_dict)

        # Save to file
        file_path = f"/home/scrapeops/axioon-scrape/Spiders/Results/{self.name}_{timestamp}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(articles_data, f, ensure_ascii=False)

        # Upload to S3
        s3_path = f"News/GO/{self.name}_{timestamp}.json"
        if upload_file(file_path, "axioon", s3_path):
            # Notify webhook only after successful S3 upload
            requests.post(
                f"{os.environ['API_IP']}/webhook/news",
                json={"records": s3_path}
            )
# from datetime import date, datetime, timedelta
# from botocore.exceptions import ClientError
# from scrapy.http import Request
# from ..items import articleItem

# from bs4 import BeautifulSoup
# import requests
# import logging
# import scrapy
# import locale
# import boto3
# import json
# import os



# def upload_file(file_name, bucket, object_name=None):
#     if object_name is None:
#         object_name = os.path.basename(file_name)

#     s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region_name="us-east-1")
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#         acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True

# now = datetime.now()
# timestamp = datetime.timestamp(now)

# locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# today = date.today().strftime("%d/%m/%Y")
# today = datetime.strptime(today, "%d/%m/%Y")

# search_limit = date.today() - timedelta(days=1)
# search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")
# main_url = "https://www.dm.com.br/ajax/noticiasCategory?offset=0&categoryId=49&amount=10"

# site_id = "029a3c5d-b0f9-42a8-9a96-eaf94216e46b"

# request = requests.get(f"{os.environ['API_IP']}/scrape/news/{site_id}")
# search_words = request.json()


# with open("/home/scrapeops/axioon-scrape/Spiders/CSS_Selectors/GO/Go_DiarioDaManha.json") as f:
#     search_terms = json.load(f)

# class GoDiarioDaManha(scrapy.Spider):
#     name = "Go_DiarioDaManha"
#     allowed_domains = ["dm.com.br"]
#     start_urls = ["https://www.dm.com.br/ajax/noticiasCategory?offset=0&categoryId=49&amount=20"]
#     INCREMENT = 0
    
#     def parse(self, response):
#         for article in response.css(search_terms['article']):
#             link = article.css(search_terms['link']).get()
#             yield Request(f"https://dm.com.br{link}", callback=self.parse_article, priority=1)
#         self.INCREMENT += 20
#         next_page = f"https://www.dm.com.br/ajax/noticiasCategory?offset={self.INCREMENT}&categoryId=49&amount=20"
#         if next_page is not None:
#             yield response.follow(next_page, callback=self.parse)
#         else:
#             print("NÃO TEM NEXT BUTTON")
            
#     def parse_article(self, response):
#         updated = response.css(search_terms['updated']).get()
#         updated = updated.split(",")[1]
#         updated = updated.split("-")[0].strip()
#         updated = updated.replace("de ", "").strip()
#         updated = datetime.strptime(updated, "%d  %B  %Y").strftime("%d/%m/%Y")
#         updated = datetime.strptime(updated, "%d/%m/%Y")
#         title = response.css(search_terms['title']).get()
#         content = response.css(search_terms['content']).getall()
#         content = BeautifulSoup(" ".join(content), "html.parser").text
#         content = content.replace("\n", " ")
#         if search_limit <= updated <= today:
#             found_names = []
#             # for paragraph in content:
#             for user in search_words['users']:
#                 if user['social_name'] in content:
#                     found_names.append({'name': user['social_name'], 'id': user['id']})
#                     item = articleItem(
#                         updated=updated,
#                         title=title,
#                         content=content,
#                         link=response.url,
#                         users=found_names,
#                         site_id=site_id
#                     )
#                     yield item
#                     if item is not None:
#                         article_dict = {
#                             "updated": item['updated'].strftime("%d/%m/%Y"),
#                             "title": item['title'],
#                             "content": [item['content']],
#                             "link": item['link'],
#                             "users": item['users'],
#                             "site_id": item['site_id']
#                         }
#                         file_path = f"/home/scrapeops/axioon-scrape/Spiders/Results/{self.name}_{timestamp}.json"
#                         if not os.path.isfile(file_path):
#                             with open(file_path, "w", encoding="utf-8") as f:
#                                 json.dump([], f)

#                         with open(file_path, "r") as f:
#                             data = json.load(f)

#                         data.append(article_dict)

#                         with open(file_path, "w", encoding="utf-8") as f:
#                             json.dump(data, f, ensure_ascii=False)
                            
#                         upload_file(f"/home/scrapeops/axioon-scrape/Spiders/Results/{self.name}_{timestamp}.json", "axioon", f"News/GO/{self.name}_{timestamp}.json")
#                         file_name = requests.post(f"{os.environ['API_IP']}/webhook/news", json={"records": f"News/GO/{self.name}_{timestamp}.json"})
#         else:
#             raise scrapy.exceptions.CloseSpider
        