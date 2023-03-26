# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .items import Fees, NAPLAN, SchoolDetails
import csv


class CsvWriterPipeline:
    def open_spider(self, spider):
        list_of_column_names = Fees.fields.keys()
        self.file = open('./fees.csv', 'w', newline='')
        self.csv_dict_writer = csv.DictWriter(
            self.file, fieldnames=list_of_column_names, extrasaction='ignore')
        self.csv_dict_writer.writeheader()

    def process_item(self, item, spider):
        self.csv_dict_writer.writerow(item)
        
    def close_spider(self, spider):
        self.file.close()


class FeesCsvWriterPipeline:

    def process_item(self, item, spider):
        if isinstance(item, Fees):
            self.list_of_column_names = Fees.fields.keys()
            self.file = open('./fees.csv', 'w', newline='')
            self.csv_dict_writer = csv.DictWriter(
                self.file, fieldnames=self.list_of_column_names, extrasaction='ignore')
            self.csv_dict_writer.writeheader()
            self.csv_dict_writer.writerow(item)
        else:
            return item

    def close_spider(self, spider):
        self.file.close()

class NAPLANCsvWriterPipeline:

    def process_item(self, item, spider):
        if isinstance(item, NAPLAN):
            self.list_of_column_names = NAPLAN.fields.keys()
            self.file = open('./NAPLAN.csv', 'w', newline='')
            self.csv_dict_writer = csv.DictWriter(
                self.file, fieldnames=self.list_of_column_names, extrasaction='ignore')
            self.csv_dict_writer.writeheader()
            self.csv_dict_writer.writerow(item)
        else:
            return item

    def close_spider(self, spider):
        self.file.close()

class SchoolDetailsCsvWriterPipeline:

    def process_item(self, item, spider):
        if isinstance(item, SchoolDetails):
            self.list_of_column_names = SchoolDetails.fields.keys()
            self.file = open('./school_details.csv', 'w', newline='')
            self.csv_dict_writer = csv.DictWriter(
                self.file, fieldnames=self.list_of_column_names, extrasaction='ignore')
            self.csv_dict_writer.writeheader()
            self.csv_dict_writer.writerow(item)
        else:
            return item

    def close_spider(self, spider):
        self.file.close()
