import sys
import asyncio

# Устанавливаем совместимый цикл событий для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from twisted.internet import asyncioreactor
asyncioreactor.install()

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from myproject.spiders.resource_spider import ResourceSpider
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
)

@inlineCallbacks
def crawl():
    runner = CrawlerRunner(get_project_settings())
    yield runner.crawl(ResourceSpider)
    reactor.callLater(900, crawl)  # Запланировать следующий запуск через 15 минут

# Запуск первого цикла
crawl()

# Запуск основного цикла событий Twisted
reactor.run()
