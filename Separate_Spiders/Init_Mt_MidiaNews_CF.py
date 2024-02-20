# from datetime import date, datetime, timedelta
# from botocore.exceptions import ClientError
# 
# from bs4 import BeautifulSoup
# import requests
# import logging
# import locale
# import boto3
# import json
# import sys
# import os

# 

# def upload_file(file_name, bucket, object_name=None):
#     """Upload a file to an S3 "bucket"

#     :param file_name: File to upload
#     :param "bucket": "bucket" to upload to
#     :param object_name: S3 object name. If not specified then file_name is used
#     :return: True if file was uploaded, else False
#     """

#     # If S3 object_name was not specified, use file_name
#     if object_name is None:
#         object_name = os.path.basename(file_name)

#     # Upload the file
#     s3_client = boto3.client('s3', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#         acl = s3_client.put_object_acl(Bucket=bucket, Key=object_name, ACL='public-read')
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True
# locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

# start_url_extension = "/index_secao.php?query_string=/&pageNum_Pagina=0&sid=1&totalRows_Pagina=55461"
# next_page = None

# now = datetime.now()
# timestamp = datetime.timestamp(now)

# today = date.today().strftime("%d/%m/%Y")
# today = datetime.strptime(today, "%d/%m/%Y")

# search_limit = date.today() - timedelta(days=1)
# search_limit = datetime.strptime(search_limit.strftime("%d/%m/%Y"), "%d/%m/%Y")

# # INIT API ROUTE
# request = requests.get(f"{os.environ['API_IP']}/scrape/news/aeee8db5-4c27-45e3-bdce-d6ee8f71ae90")
# search_words = request.json()

# item = []

# while True:
# 	url = "https://dripcrawler.p.rapidapi.com/"
# 	main_url = "https://www.midianews.com.br"

# 	if next_page is not None:
# 		start_url_extension = next_page
 
# 	payload_url = main_url + start_url_extension

# 	payload = {
# 		"url": f"{payload_url}",
# 		"javascript_rendering": "False"
# 	}
# 	headers = {
# 		"content-type": "application/json",
# 		"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
# 		"X-RapidAPI-Host": "dripcrawler.p.rapidapi.com"
# 	}

# 	response = requests.post(url, json=payload, headers=headers)

# 	html = response.json()['extracted_html']

# 	bs = BeautifulSoup(html, 'html.parser')

# 	article_banner = bs.find_all("p", {"class": "titulo-secao"})
# 	article_banner = BeautifulSoup(str(article_banner), 'html.parser')
# 	article_banner = article_banner.find_all("a")
 
# 	links = []
 
# 	for link in article_banner:
# 		links.append(link['href'])
	
# 		url = "https://dripcrawler.p.rapidapi.com/"

# 		article_payload_url = link["href"]

# 		payload = {
# 		"url": f"https://www.midianews.com.br{article_payload_url}",
# 		"javascript_rendering": "False"
# 		}
# 		headers = {
# 			"content-type": "application/json",
# 			"X-RapidAPI-Key": os.getenv("X_RAPIDAPI_KEY"),
# 			"X-RapidAPI-Host": "dripcrawler.p.rapidapi.com"
# 		}

# 		article_response = requests.post(url, json=payload, headers=headers)

# 		article_html = article_response.json()['extracted_html']

# 		article_bs = BeautifulSoup(article_html, 'html.parser')
  
# 		article_updated = article_bs.find("div", {"class": "row espaco-conteudo"})
# 		article_updated = BeautifulSoup(str(article_updated), 'html.parser')
# 		article_updated = article_updated.find_all("span")
# 		article_updated = article_updated[1].text
# 		article_updated = article_updated.split("|")[0]
# 		article_updated = article_updated.replace(".", "/")
# 		article_updated = article_updated.strip()
# 		article_updated = datetime.strptime(article_updated, "%d/%m/%Y")
  
# 		article_title = article_bs.find("h1").text

# 		article_paragraphs = []

# 		article_content = article_bs.find("div", {"id": "texto", "class": "texto"})
# 		article_content = BeautifulSoup(str(article_content), 'html.parser')
# 		article_content = article_content.find_all("p")
# 		for paragraph in article_content:
# 			text = paragraph.text.replace("\n", "")
# 			text = text.replace("\xa0", " ")
# 			text = text.strip()
# 			article_paragraphs.append(text)
# 			article_paragraphs = [string for string in article_paragraphs if string != ""]

# 		if search_limit <= article_updated <= today:

# 			updated_str = article_updated.strftime("%d/%m/%Y")

# 			found_names = []
# 			for paragraph in article_paragraphs:
# 				for user in search_words['users']:
# 					if user['social_name'] in paragraph:
# 						found_names.append({'name': user['social_name'], 'id': user['id']})
# 						item.append({
# 							"updated": updated_str,
# 							"title": article_title,
# 							"content": article_paragraphs,
# 							"link": article_payload_url,
# 							"users": found_names
# 						})

# 		else:
# 			unique_item = list({v['link']:v for v in item}.values())
# 			with open("/home/scrapeops/axioon-scrape/Spiders/Results/Mt_MidiaNews.json", "w") as f:
# 				json.dump(unique_item, f, indent=4, ensure_ascii=False)
# 			upload_file("/home/scrapeops/axioon-scrape/Spiders/Results/Mt_MidiaNews.json", "axioon", f"News/MT/Mt_MidiaNews_{timestamp}.json")
# 			file_name = requests.post(f"{os.environ['API_IP']}/webhook/news", json={"records": f"News/MT/Mt_MidiaNews_{timestamp}.json"})
# 			sys.exit()
    
# 	next_page = bs.find("a", {"rel": "nofollow"}).get("href")
 
# 	start_url_extension = next_page