from scrapy.item import Field
from scrapy.item import Item
from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from scrapy.loader.processors import MapCompose
from scrapy.linkextractors import LinkExtractor
from scrapy.loader import ItemLoader
from bs4 import BeautifulSoup
import re

# STEPS:
"""
1. Establishing the classes or abstractions where we will store the data we will need.
2. Establishing our Scraper.
*From now on, we are inside the Scraper.*
3. Give it a name.
4. Custom settings.
5. Seed URL.
6. Rules.
7. parser(s). To extract data.
"""

class Comment(Item):
    hotel = Field()
    name = Field()
    score = Field()
    title = Field()
    description = Field()
    
class TripAdvisorCrawler(CrawlSpider):
    name = "TripAdvisorCrawler"
    custom_settings = {
        "USER_AGENT":"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/84.0.2",
        "CLOSESPIDER_PAGECOUNT": 50,
        #"CLOSESPIDER_ITEMCOUNT": 310,
        "FEED_EXPORT_ENCODING":"utf-8",
        "DEPTH_PRIORITY" : 1,
        "SCHEDULER_DISK_QUEUE" : 'scrapy.squeues.PickleFifoDiskQueue',
        "SCHEDULER_MEMORY_QUEUE" : 'scrapy.squeues.FifoMemoryQueue',
    }
    
    allowed_domains = ["tripadvisor.com.mx"]
    start_urls = ["https://www.tripadvisor.com.mx/Hotels-g150771-La_Paz_Baja_California-Hotels.html"] # The SEED URL.
    download_delay = 1
    
    rules = (
        
        # The callback ones. Here, using these rules we are going to extract the data:
        Rule( # Rule for the VERTICAL Scraping in the main level (per hotel). TO ACCESS TO THE INFO INSIDE EVERY HOTEL.
            LinkExtractor(
                allow = r"/Hotel_Review-",
                restrict_xpaths = ["//div[@class='listing_title']/a"]
            ), follow = True, callback = "parser_opinions"
        ),
        
        Rule( # Rule for the PAGINATION in the secondary level (reviews).
            LinkExtractor(
                allow = r"-or\d+-",
            ), follow = True, callback = "parser_opinions"
        ),
        
        # No callbacks since these rules will be specifically to enter to every page:
        Rule( # Rule for PAGINATION in the main Level (hotels). TO GO TROUGH EVERY PAGE.
            LinkExtractor(
                allow = r"-oa\d+-"
            ), follow = True
        ),
        
    )
    
    def parser_opinions(self, response):
        #selector = Selector(response)
        BS_object = BeautifulSoup(response.body, 'lxml')
        comment_blocks = BS_object.find_all("div",{"data-test-target":"HR_CC_CARD"})
        #comment_blocks = selector.xpath("//div[@data-test-target='HR_CC_CARD']")
        #Hotel = selector.xpath("//h1[@id='HEADING']/text()").get()
        Hotel = BS_object.find("h1",{"id":"HEADING"}).text
        #for comment_block, cb in zip(comment_blocks, block_2):
        for cb in comment_blocks:
            #Name = comment_block.xpath("//a[contains(@class, 'ui_header') and contains(@href, 'Profile')]/text()").get()
            Name = cb.find('a', {"class":re.compile(r"ui_header"), "href":re.compile(r"Profile")}).get_text()
            Score = cb.find("span", {"class":re.compile(r"ui_bubble")}).attrs["class"][-1][-2:]
            Title = cb.find("div", {"data-test-target":"review-title"}).find("span").get_text()
            #Description = comment_block.xpath("//q//text()").get()
            Description = cb.find('q').text
            
            #print("#"*60)
            #print(Hotel)
            #print(Name)
            #print(Score)
            #print(Title)
            #print(Description)
            
            item = ItemLoader(Comment(), cb)
            item.add_value("hotel", Hotel)
            item.add_value("name", Name)
            item.add_value("score", int(Score))
            item.add_value("title", Title)
            item.add_value("description", Description)
            
            yield item.load_item()