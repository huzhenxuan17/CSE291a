from content import bloom_filter
from content import mem_table
from content import ss_table


class Column():
    def __init__(self):
        self.bf = bloom_filter.BloomFilter()
        self.st = ss_table.SSTable()
        self.mt = mem_table.MEMTable()

    def add(self, key, value):
        self.mt.add(key, value)

    def get(self, key):
        """
        read key in data:
            step1: read in mt
            step2: read in bf
            step3: read in st
        :param key:
        :return:
        """
        self.mt.get(key)

    def delete(self, key):
        pass

    @staticmethod
    def dump():
        """
        1. save mem_table to ss_table
        2. update bloom_filter
        :return:
        """
