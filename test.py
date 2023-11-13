from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
import re

driver = webdriver.Firefox()
driver.get("https://www.jornalopcao.com.br/categoria/politica/?pg=1")

page_source = driver.page_source

soup = BeautifulSoup(page_source)

article = soup.find("div", class_="template-part-component-feed-item")

article_link = article.find("a", class_="main_link").get("href")

print("article_link: ", article_link)