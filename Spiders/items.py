# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class articleItem(Item):
    title = Field()
    updated = Field()
    content = Field()
    link = Field()
    users = Field()
    site_id = Field()