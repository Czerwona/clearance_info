import scrapy
import pandas as pd
import re
        
class ClearanceSpider(scrapy.Spider):
    name = "clearance"

    def start_requests(self):
        base = 'http://ogc.osd.mil/doha/industrial/'
        urls = [base + str(i) + '.html' for i in range(1996,2018)]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
         
        case_keywords = [i.css('div.keywords::text').extract() for i in response.css('div.case')]
        case_number = [i.css('div.casenum').css('a::text').extract() for i in response.css('div.case')]
        case_date = [i.css('p.date::text').extract() for i in response.css('div.case')]
        case_digest = [i.css('p.digest::text').extract() for i in response.css('div.case')]
        
        data = [[case_number[i], case_date[i], case_keywords[i], case_digest[i][:]] for i in range(len(case_number))]
        
        df = pd.DataFrame(data)
        df.columns = ['case_number','date','keywords','digest']
        file_name = re.findall(r'.*([1-3][0-9]{3})', response.url)[0]
        df.to_pickle('pickle/'  + file_name + '.pickle') 
