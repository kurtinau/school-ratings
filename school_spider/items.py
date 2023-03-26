# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SchoolSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class Fees(scrapy.Item):
    school_name = scrapy.Field()
    kinder = scrapy.Field()
    prep = scrapy.Field()
    grade1 = scrapy.Field()
    grade2 = scrapy.Field()
    grade3 = scrapy.Field()
    grade4 = scrapy.Field()
    grade5 = scrapy.Field()
    grade6 = scrapy.Field()
    year7 = scrapy.Field()
    year8 = scrapy.Field()
    year9 = scrapy.Field()
    year10 = scrapy.Field()
    year11 = scrapy.Field()
    year12 = scrapy.Field()
    boarding = scrapy.Field()
    international = scrapy.Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for keys, _ in self.fields.items():
            self[keys] = '0'
        if kwargs:
            if len(kwargs.values()) == 1:
                for value in kwargs.values():
                    self['school_name'] = value


class NAPLAN(scrapy.Item):
    school_name = scrapy.Field()
    grade = scrapy.Field()
    year = scrapy.Field()
    writing = scrapy.Field()
    spelling = scrapy.Field()
    grammar = scrapy.Field()
    numeracy = scrapy.Field()

    # def __init__(self, *args, **kwargs):
    #     for keys, _ in self.fields.items():
    #         self[keys] = '0'
    #     if kwargs:
    #         if len(kwargs.values()) == 1:
    #             for value in kwargs.values():
    #                 self['school_name'] = value

class SchoolDetails(scrapy.Item):
    school_name = scrapy.Field()
    sector = scrapy.Field()
    level = scrapy.Field()
    gender = scrapy.Field()
    religioin = scrapy.Field()

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     if kwargs:
    #         if len(kwargs.values()) == 1:
    #             for value in kwargs.values():
    #                 self['school_name'] = value