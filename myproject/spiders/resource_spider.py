import pytz
import mysql.connector
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
from dateparser import parse
import time
from dotenv import load_dotenv
import emoji
from datetime import datetime
import re
from lxml.html import fromstring
import bs4
import os
from mysql.connector import Error
import logging
from logging.handlers import RotatingFileHandler
from scrapy.utils.log import configure_logging
from scrapy.loader import ItemLoader


load_dotenv()
class ResourceSpider(CrawlSpider):

    name = 'resource_spider'

    def __init__(self, resources=None,  spider_name=None, *args, **kwargs):
        self.spider_name = spider_name or self.name
        super().__init__(*args, **kwargs)

        log_file = f'{spider_name}.log'
        handler = RotatingFileHandler(
            log_file, maxBytes=1024 * 1024, backupCount=3
        )
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Создаем логгер с именем паука
        self.custom_logger = logging.getLogger(spider_name)
        self.custom_logger.setLevel(logging.INFO)
        self.custom_logger.addHandler(handler)
        self.custom_logger.addHandler(console_handler)

        self.setup_scrapy_logging(spider_name, handler, console_handler)
        

        # Передача ресурсов
        self.resources = resources

        self.cursor_1 = None
        self.conn_2 = None
        self.cursor_2 = None
        self.conn_3 = None
        self.cursor_3 = None
        self.start_urls = []


        try: # подключение к таблице temp_items и temp_items_link
            self.conn_2 = mysql.connector.connect(
                host=os.getenv("DB_HOST_1"),
                user=os.getenv("DB_USER_1"),
                password=os.getenv("DB_PASSWORD_1"),
                database=os.getenv("DB_DATABASE_1"),
                port=os.getenv("DB_PORT_1"),
                charset='utf8mb4',
                collation='utf8mb4_general_ci'

            )

            if self.conn_2.is_connected():
                self.cursor_2 = self.conn_2.cursor()
                self.custom_logger.info(f'Есть подключение к БД: {spider_name}')

            if resources:
                self.resources = resources
                self.start_urls = [resource[2].split(',')[0].strip() for resource in self.resources]
                self.allowed_domains = [urlparse(url).netloc.replace('www.', '') for url in self.start_urls]
                self.custom_logger.info(f'Allowed domains: {self.allowed_domains}')
                deny = [r'//kabar.kg/arkhiv-kategorii/', r'//kabar.kg/archive/', r'//bilimdiler.kz/tags/', r'//kerekinfo.kz/tag/',
                        r'//abai.kz/archive/', r'//infor.kz/avto/', r'//shop.kz/catalog/', r'//shop.kz/offers/']

                # Создание правил для каждого ресурса
                self.rules = (
                    Rule(LinkExtractor(restrict_xpaths="//a", deny=deny), callback='parse_links', follow=True),
                )

                super()._compile_rules()

            else:
                self.log("No resources found, spider will close.")
                self.crawler.engine.close_spider(self, f'Нету данных в бд {spider_name}')


        except Error as e:
            self.log(f"Error connecting to MySQL: {e}")
            self.logger.info('Нет подключение к БД')
            # Переключаемся на временный паук чтобы закрыть паука и запустить через 30 мин
            self.name = "temporary_spider"
            self.start_urls = ["http://example.com"]
            self.rules = ()
            self._compile_rules()

    def setup_scrapy_logging(self, spider_name, handler, console_handler):
        """
        Настраиваем Scrapy логгер для перенаправления всех сообщений в файл паука.
        """
        # Отключаем глобальное конфигурирование логирования Scrapy
        configure_logging(install_root_handler=False)

        # Перенаправляем стандартные логи Scrapy в наш кастомный логгер
        scrapy_logger = logging.getLogger('scrapy')
        scrapy_logger.propagate = False
        scrapy_logger.setLevel(logging.INFO)
        scrapy_logger.addHandler(handler)
        scrapy_logger.addHandler(console_handler)


    def parse_links(self, response):
        # Получаем текущий URL
        current_url = response.url
        if any(current_url.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx']):
            # self.logger.info(f'Пропускаем неподходящий ссылку: {current_url}')
            return
        self.cursor_2.execute("SELECT 1 FROM temp_items_link WHERE link = %s", (current_url,))
        if self.cursor_2.fetchone() is not None:
            self.logger.info(f'ссылка существует {current_url}')
            return

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
            loader = ItemLoader(item=MyItem(), response=response)
            title_t = response.xpath(f'normalize-space({resource_info[5]})').get() #получение заголовок новостей
            if not title_t:
                self.logger.info(f"Заголовок отсутствует для {current_url}")
                return
            content = response.xpath(resource_info[4]).getall() #получение контента новостей
            content = self.clean_text(content)
            if not content or all(item.isspace() for item in content):
                self.logger.info(f"Контент отсутствует для {current_url}")
                return
            title = self.replace_unsupported_characters(title_t)
            date = response.xpath(resource_info[6]).get()
            if not date:
                self.logger.info(f"Дата отсутствует для {current_url}")
                return
            #получение даты новостей
            date = self.parse_date(date, resource_info[7])
            if not date:
                self.logger.info(f"Дата отсутствует для {current_url}")
                return

            n_date = date #дата публикаций новостей
            nd_date = int(time.mktime(date.timetuple())) #дата публикаций новостей UNIX формате
            not_date = date.strftime('%Y-%m-%d') #дата публикаций новостей
            s_date = int(time.time()) #дата поступление новостей в таблицу


            self.store_news(resource_id, title, current_url, nd_date, content, n_date, s_date, not_date)


    def store_news(self, resource_id, title, current_url, nd_date, content, n_date, s_date, not_date):
        # Проверка соединения перед выполнением операций
        if not self.conn_2.is_connected():
            try:
                self.logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                self.conn_2.reconnect(attempts=3, delay=5)
                self.logger.info("Соединение восстановлено")
            except mysql.connector.Error as err:
                self.logger.warning(f"Ошибка переподключения: {err}")
                return  # Прекращаем выполнение, если не удалось переподключиться

        # Проверка наличия ссылки в таблице
        self.cursor_2.execute(
            "SELECT COUNT(*) FROM temp_items WHERE link = %s",
            (current_url,)
        )
        (count,) = self.cursor_2.fetchone()

        if count == 0:
            # Если ссылка не найдена, добавляем её в таблицу
            status = ''
            self.cursor_2.execute(
                "INSERT INTO temp_items (res_id, title, link, nd_date, content, n_date, s_date, not_date, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (resource_id, title, current_url, nd_date, content, n_date, s_date, not_date, status)
            )
            self.conn_2.commit()
            self.custom_logger.warning(f'Новость добавлена в базу: {current_url}')
        else:
            # Если ссылка уже существует
            self.custom_logger.info(f'Ссылка уже существует в базе TEMP: {current_url}')

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

    def parse_date(self, date_str, convert_date):
        date_str = str(date_str) if date_str else ''
        date_str = re.sub(r'-го|г\.|\bPublish\w*|\bжыл\w*|тому|\bавтор\w*|', '', date_str)
        languages = ['ru', 'kk', 'en']
        if not convert_date:  # Присваиваем список по умолчанию
            DATE_ORDERS = ["YMD", "DMY", "MYD"]
        else:
            if isinstance(convert_date, str): #Если переменная содержит строку (например, "YMD"), превращаем её в список
                DATE_ORDERS = [convert_date]
            else:
                DATE_ORDERS = convert_date
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

    def close(self, reason):
        if self.cursor_2:
            self.cursor_2.close()
        if self.cursor_3:
            self.cursor_3.close()
        if self.conn_2:
            self.conn_2.close()
        if self.conn_3:
            self.conn_3.close()


