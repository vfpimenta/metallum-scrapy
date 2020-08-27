# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class MetallumItem(Item):
    url = Field(serializer=str)
    name = Field(serializer=str)
    description = Field(serializer=str)
    category = Field(serializer=str)
    brand = Field(serializer=str)
    navigation = Field(serializer=list)
    #vendor_name = Field(serializer=str)
    old_price = Field(serializer=float)
    current_price = Field(serializer=float)
    discount_price = Field(serializer=float)
    main_image = Field(serializer=str)
    secondary_image = Field(serializer=str)
    secondary_images = Field(serializer=list)
    features = Field(serializer=list)
    #dimensions = Field(serializer=dict)