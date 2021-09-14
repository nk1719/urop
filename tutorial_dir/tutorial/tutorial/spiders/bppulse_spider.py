import scrapy
import csv
from time import sleep
from datetime import datetime
from scrapy.crawler import CrawlerProcess, CrawlerRunner
from scrapy.utils.project import get_project_settings     
from twisted.internet import reactor
from twisted.internet.task import deferLater

class BppulseSpider(scrapy.Spider):
    name = 'bppulse'
    start_urls = ['https://network.bppulse.co.uk/ajax/posts']
        
    def parse(self, response):
        page = response.url.split("/")[-2]
        ctime = format(datetime.now(), '%d_%m_%Y___%H_%M')
        
        data = response.body
        mytext = data
        result = mytext[24:-1]
        main = eval(result)
        
        stats = []
        serials, lat, lon, post, name  = [], [], [], [], []
        for dic in main:
            for dic2 in dic["Sockets"]:
                if dic2["Status"] == '':
                    stats.append('NA')
                else:
                    stats.append(dic2["Status"])
                name.append(dic2["Name"])
                
            for i in range(len(dic["Sockets"])):
                serials.append((dic["Serial"]))
                lat.append(dic["Latitude"])
                lon.append(dic["Longitude"])
                post.append(dic["Postcode"])


        
        with open(f'bppulse_{ctime}.csv', "w") as csvfile:
            writer = csv.writer(csvfile)
            heads = ["Serial", "Latitude", "Longitude", "Postcode", "Socket Name", "Status"]
            writer.writerow([g for g in heads])
            for value in range(len(serials)):
                writer.writerow([serials[value], lat[value], lon[value], post[value], name[value], stats[value]])


def sleep(self, *args, seconds):
    """Non blocking sleep callback"""
    return deferLater(reactor, seconds, lambda: None)

process = CrawlerProcess(get_project_settings())

def _crawl(result, spider):
    deferred = process.crawl(spider)
    deferred.addCallback(lambda results: print('waiting 10 minutes before restart...'))
    deferred.addCallback(sleep, seconds=580)
    deferred.addCallback(_crawl, spider)
    return deferred


_crawl(None, BppulseSpider)
process.start()