import mysql.connector
import pytz
from mysql.connector import Error
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
import dateparser
from dateparser import parse
import time
import scrapy
import os
from dotenv import load_dotenv
import emoji
from datetime import datetime
import re
from lxml.html import fromstring
import bs4


def parse_date(date_str):
    date_str = str(date_str) if date_str else ''
    date_str = re.sub(r'-го|г\.|Published|\bжыл\w*|', '', date_str)
    languages = ['ru', 'kk', 'en']
    DATE_ORDERS = ["YMD"]
    date_formats = ['']
    UTC = pytz.UTC
    for date_order in DATE_ORDERS:
        date = parse(date_str,
                     languages=languages,
                     date_formats=date_formats,
                     settings={"DATE_ORDER": date_order},
                     )
        if date:
            date_with_utc = date.replace(tzinfo=UTC)
            if date_with_utc <= datetime.now().replace(tzinfo=UTC):
                return date_with_utc
    return None

date = '2024-12-07T14:26:00+06:00'
date = parse_date(date)
print(date)
n_date = date
nd_date = int(time.mktime(date.timetuple()))
not_date = date.strftime('%Y-%m-%d')
s_date = int(time.time())

print(not_date, s_date, nd_date, n_date)

