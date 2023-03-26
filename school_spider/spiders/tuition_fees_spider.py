from pathlib import Path
import scrapy
import csv

class TuitionFeesSpider(scrapy.Spider):
    name = "fees"

    start_urls = [
        'https://www.goodschools.com.au/compare-schools/in-armadale-3144/the-king-david-school'
    ]

    def parse_fees(self, response):
        fees_header = response.css('table.table').xpath('.//td[1]/text()').getall()
        fees_content = response.css('table.table').xpath('.//td[2]/text()').getall()
        with open('fees.csv', 'w', newline='') as csvfile:
            fees_writer = csv.writer(csvfile, delimiter=',')
            fees_writer.writerow(fees_header)
            fees_writer.writerow(fees_content)

    def parse_NAPLAN(self, response):
        NAPLAN_header = response.css(
            'table.table').xpath('.//th/text()').getall()
        with open('NAPLAN.csv', 'w', newline='') as csvfile:
            NAPLAN_writer = csv.writer(csvfile, delimiter=',')
            NAPLAN_writer.writerow(NAPLAN_header)
            for selector in response.css('table.table').xpath('.//tr[position()>1]'):
                NAPLAN_writer.writerow(selector.xpath('.//td/text()').getall())

    def parse(self, response):
        school_details_header = response.css(
            'div.card-body').xpath('.//div/p[1]/text()').getall()
        school_details_content = response.css(
            'div.card-body').xpath('.//div/p[2]/text()').getall()
        with open('school_details.csv', 'w', newline='') as csvfile:
            school_writer = csv.writer(csvfile, delimiter=',')
            school_writer.writerow(school_details_header)
            school_writer.writerow(school_details_content)
        for page_selecotr in response.css('ul.nav').xpath('.//li'):
            page_name = page_selecotr.xpath('.//a/text()').get().strip()
            next_page = page_selecotr.xpath('.//a/@href').get()
            if page_name == 'Fees':
                yield response.follow(next_page, callback=self.parse_fees)
            if page_name == 'NAPLAN':
                yield response.follow(next_page, callback=self.parse_NAPLAN)
