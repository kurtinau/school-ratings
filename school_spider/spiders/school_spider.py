import scrapy
import csv
from ..items import Fees, NAPLAN, SchoolDetails
from ..util import convert2Dict


class schoolSpider(scrapy.Spider):
    name = "school"
    start_urls = [
        'https://www.goodschools.com.au/compare-schools/search?keywords=',
    ]

    def parse_fees(self, response, school_name):
        table_title_list = response.css('section.section h3::text').getall()
        if table_title_list:
            primary_high_school_indexs = []
            boarding_indexs = []
            international_indexs = []
            for index, title in enumerate(table_title_list):
                if 'primary' in title.lower() or 'high' in title.lower():
                    primary_high_school_indexs.append(index)
                if 'boarding' in title.lower():
                    boarding_indexs.append(index)
                if 'international' in title.lower():
                    international_indexs.append(index)
            if primary_high_school_indexs:
                p_h_item = Fees(school_name=school_name)
                fees_dict = {}
                for i in primary_high_school_indexs:
                    fees_list = response.css('section.section').xpath(
                        f'.//h3[1]/following-sibling::div[{i+1}]//td/text()').getall()
                    if fees_list:
                        fees_dict = convert2Dict(fees_list)
                for key in fees_dict:
                    p_h_item[key] = fees_dict[key]
                yield p_h_item
            if boarding_indexs:
                boarding_item = Fees(school_name=school_name)
                fees_dict = {}
                for i in boarding_indexs:
                    fees_list = response.css('section.section').xpath(
                        f'.//h3[1]/following-sibling::div[{i+1}]//td/text()').getall()
                    if fees_list:
                        fees_dict = convert2Dict(fees_list)
                for key in fees_dict:
                    boarding_item[key] = fees_dict[key]
                boarding_item['boarding'] = '1'
                yield boarding_item
            if international_indexs:
                inter_item = Fees(school_name=school_name)
                fees_dict = {}
                for i in international_indexs:
                    fees_list = response.css('section.section').xpath(
                        f'.//h3[1]/following-sibling::div[{i+1}]//td/text()').getall()
                    if fees_list:
                        fees_dict = convert2Dict(fees_list)
                for key in fees_dict:
                    inter_item[key] = fees_dict[key]
                inter_item['international'] = '1'
                yield inter_item

    def parse_NAPLAN(self, response, school_name):
        NAPLAN_header = response.css(
            'table.table').xpath('.//th/text()').getall()
        with open('naplan.csv', 'a', newline='') as csvfile:
            NAPLAN_writer = csv.writer(csvfile, delimiter=',')
            for selector in response.css('table.table').xpath('.//tr[position()>1]'):
                td_selectors = selector.xpath('.//td')
                row_content = [school_name]
                if td_selectors:
                    for td_selector in td_selectors:
                        row_content.append(td_selector.xpath('.//text()').get(default='0').strip())
                    NAPLAN_writer.writerow(row_content)

    def parse_detail_page(self, response, school_name):
        # school_name = response.css(
        #     'div.sia-client-profile-intro h1::text').get()
        callback_args_dict = {'school_name': school_name}
        school_details_header = response.css(
            'div.card-body').xpath('.//div/p[1]/text()').getall()
        school_details_content = response.css(
            'div.card-body').xpath('.//div/p[2]/text()').getall()
        school_details_content.insert(0, school_name)
        with open('detail.csv', 'a', newline='') as csvfile:
            school_writer = csv.writer(csvfile, delimiter=',')
            # school_writer.writerow(school_details_header)
            school_writer.writerow(school_details_content)

        for page_selecotr in response.css('ul.nav').xpath('.//li'):
            page_name = page_selecotr.xpath('.//a/text()').get().strip()
            next_page = page_selecotr.xpath('.//a/@href').get()
            if page_name == 'Fees':
                yield response.follow(next_page, callback=self.parse_fees, cb_kwargs=callback_args_dict)
            if page_name == 'NAPLAN':
                yield response.follow(next_page, callback=self.parse_NAPLAN, cb_kwargs=callback_args_dict)

    def parse(self, response):
        # follow school detail link
        school_link_list = response.css(
            'div#search-results div.row div.media-body').xpath('.//a/@href').getall()
        school_name_list = response.css(
            'div#search-results div.row div.media-body').xpath('.//a/h5/text()').getall()
        for link, name in zip(school_link_list, school_name_list):
            yield response.follow(link, callback=self.parse_detail_page, cb_kwargs={'school_name': name})

        # follow pagination link
        pagination_links = response.xpath('//li/a[@rel="next"]')
        yield from response.follow_all(pagination_links, self.parse)