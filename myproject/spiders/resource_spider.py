# from twisted.internet import asyncioreactor
# asyncioreactor.install()

import mysql.connector
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



load_dotenv()
class ResourceSpider(CrawlSpider):
    name = 'resource_spider'

    def __init__(self, *args, **kwargs):
        super(ResourceSpider, self).__init__(*args, **kwargs)
        self.conn_1 = None
        self.cursor_1 = None
        self.start_urls = []
        try:
            self.conn_1 = mysql.connector.connect(
                host=os.getenv("DB_HOST_1"),
                user=os.getenv("DB_USER_1"),
                password=os.getenv("DB_PASSWORD_1"),
                database=os.getenv("DB_DATABASE_1"),
                port=os.getenv("DB_PORT_1"),
                charset='utf8mb4',
                collation='utf8mb4_general_ci'

            )

            if self.conn_1.is_connected():
                self.cursor_1 = self.conn_1.cursor()
                print('Есть подключение к БД')

                # Загрузка ресурсов из базы данных
                self.cursor_1.execute(
                    "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut "
                    "FROM resource "
                    "WHERE status = %s AND bottom_tag IS NOT NULL AND bottom_tag <> '' "
                    "AND title_cut IS NOT NULL AND title_cut <> '' "
                    "AND date_cut IS NOT NULL AND date_cut <> ''"
                    "AND RESOURCE_STATUS = %s",
                        ('spider_scrapy', 'WORK')
                )
                self.resources = self.cursor_1.fetchall()

                self.start_urls = [resource[2].split(',')[0].strip() for resource in self.resources]
                self.allowed_domains = [urlparse(url).netloc.replace('www.', '') for url in self.start_urls]
                print(self.allowed_domains)

                # Создание правил для каждого ресурса
                self.rules = (
                    Rule(LinkExtractor(restrict_xpaths="//a"), callback='parse_links', follow=True, process_links=self.limit_links,),
                )

                super()._compile_rules()


        except Error as e:
            self.log(f"Error connecting to MySQL: {e}")
            print('Нет подключение к БД')
            # Переключаемся на временный паук
            self.name = "temporary_spider"
            self.start_urls = ["http://example.com"]
            self.rules = ()
            self._compile_rules()

    def limit_links(self, links):
        # Ограничиваем количество ссылок до, например, 10
        return links[:5000]

    def parse_links(self, response):
        # Получаем текущий URL
        current_url = response.url
        self.cursor_1.execute("SELECT 1 FROM temp_items_link WHERE link = %s", (current_url,))
        if self.cursor_1.fetchone() is not None:
            print(f'ссылка существует {current_url}')
            return
        print(f'Проверка контента из {current_url}')
        parsed_current_url = urlparse(current_url)
        current_netloc = parsed_current_url.netloc.replace('www.', '')
        #Ищем RESOURCE_ID для текущего URL
        resource_id = None
        resource_info = None
        for resource in self.resources:
            first_url = resource[2].split(',')[0].strip()
            parsed_first_url = urlparse(first_url)
            first_netloc = parsed_first_url.netloc.replace('www.', '')
            if first_netloc == current_netloc:
                resource_id = resource[0]
                resource_info = resource
                break

        if resource_id:
            title_t = response.xpath(f'normalize-space({resource_info[5]})').get()
            if not title_t:
                self.logger.warning(f"Заголовок отсутствует для {current_url}")
                return
            content = response.xpath(resource_info[4]).getall()
            content = self.clean_text(content)
            if not content or all(item.isspace() for item in content):
                self.logger.warning(f"Контент отсутствует для {current_url}")
                return
            title = self.replace_unsupported_characters(title_t)
            date = response.xpath(resource_info[6]).get()
            date = self.parse_date(date)
            if not date:
                self.logger.warning(f"Дата отсутствует для {current_url}")
                return
            n_date = date
            nd_date = int(time.mktime(date.timetuple()))
            not_date = date.strftime('%Y-%m-%d')
            s_date = int(time.time())


            self.store_news(resource_id, title, current_url, nd_date, content, n_date, s_date, not_date)
            self.store_link(current_url)

    def store_link(self, current_url):  # сохраняем ссылки в бд
        self.cursor_1.execute(
            "INSERT INTO temp_items_link (link) VALUES (%s)",
            (current_url,)
                            )
        self.conn_1.commit()

    def store_news(self, resource_id, title, current_url, nd_date, content, n_date, s_date, not_date):
        status = 'NULL'
        self.cursor_1.execute(
            "INSERT INTO temp_items (res_id, title, link, nd_date, content, n_date, s_date, not_date, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (resource_id, title, current_url, nd_date, content, n_date, s_date, not_date, status)
                            )
        self.conn_1.commit()

    def replace_unsupported_characters(self, text):
        text = str(text) if text else ''
        return emoji.replace_emoji(text, replace='?')

    def clean_text(self, parsed_fields: list[str]) -> str | int:
        """Function that removes junk html tags and performs some text normalization
        Very similar to what Sphinx Search does in current configuration.
        """
        if not parsed_fields:
            return ""
        content = " ".join(parsed_fields)
        content = content.replace("'", '"').strip()
        KEYWORD_SELECTORS = ["//@alt", "//@title", "//@content", "//@data-body", "//@body"]
        keywords = {
            k for selector in KEYWORD_SELECTORS for k in fromstring(content).xpath(selector)
        }

        resp = bs4.BeautifulSoup(content, features="html.parser")

        for el in resp.findAll("script"):
            el.decompose()

        for el in resp.findAll("style"):
            el.decompose()

        for el in resp.findAll("img"):
            el.decompose()

        for comment in resp.findAll(string=True):
            if isinstance(comment, bs4.element.Comment):
                comment.extract()
        try:
            content = " ".join(resp.findAll(string=True))
        except AttributeError:
            content = resp.text

        content = content + " " + " ".join(keywords)

        content = content.replace("\N{SOFT HYPHEN}", "")
        content = re.sub(r"\\+", r"\\", content)
        content = re.sub(r"\\n|_", " ", content)

        content = re.sub(r"\s+", " ", content)
        content = emoji.demojize(content)
        return content

    def parse_date(self, date_str):
        # Определите формат русской даты (дд.мм.гггг)
        russian_date_pattern = r'\d{2}\.\d{2}\.\d{4}'

        # Определите формат ISO даты (гггг-мм-ддThh:mm:ss+zz:zz)
        iso_date_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\+\d{2}:\d{2})?'

        date_str = str(date_str) if date_str else ''
        if re.search(r'-го|г\.|жылдың|Published ', date_str):
            date_str = re.sub(r'[-го|г\.|жылдың|Published|,]', '', date_str)
        else:
            date_str = date_str
        # Проверка формата даты и парсинг
        if re.fullmatch(russian_date_pattern, date_str):
            # Преобразование русской даты в стандартный формат ISO для `dateparser`
            clean_date_str = date_str + 'T00:00:00'  # Добавляем время для совместимости
            return parse(clean_date_str, languages=['ru'])
        elif re.fullmatch(iso_date_pattern, date_str):
            # Прямой парсинг даты в формате ISO
            return parse(date_str)
        else:
            return parse(date_str)
    def close(self, reason):
        if self.cursor_1:
            self.cursor_1.close()
            self.cursor_1 = None  # Установка в None для предотвращения повторного закрытия

        if self.conn_1:
            try:
                if self.conn_1.is_connected():
                    self.conn_1.close()
            except Exception as e:
                self.logger.error(f"Error closing connection conn_1: {e}")
            finally:
                self.conn_1 = None  # Установка в None для предотвращения повторного закрытия
