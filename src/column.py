from content import mem_table
from content import ss_table

DELETE_FLAG = "/"

class Column():
    def __init__(self):
        self.bigFile = []
        self.smallFile = []
        self.mt = mem_table.MEMTable()

    def add(self, key, value):
        persistence_flag = self.mt.add(key, value)
        if persistence_flag:
            self.persistence()

    def delete(self, key):
        persistence_flag = self.mt.add(key, DELETE_FLAG)
        if persistence_flag:
            self.persistence()

    def get(self, key):
        """
        read key in data:
            step1: read in mt
            step2: read in bf
            step3: read in st
        :param key:
        :return:
        """
        result = None
        if key in self.mt.mem_table:
            result = self.mt.mem_table[key]
        if not result:
            for sstable in reversed(self.smallFile):
                if sstable.bf.check(key):
                    result = sstable.get(key)
                    if result is not None:
                        break
        if not result:
            for sstable in reversed(self.bigFile):
                if sstable.bf.check(key):
                    result = sstable.get(key)
                    if result is not None:
                        break
        if result is None:
            return None
        if result == DELETE_FLAG:
            return None
        return result

    def persistence(self):
        """
        1. save mem_table to ss_table
        2. update bloom_filter
        :return:
        """
        new_file = self.mt.mem_table
        file_name = "tmp"
        st = ss_table.SSTable(new_file, file_name) # inside : update bloom filter
        self.smallFile.append(st)
        self.mt.clear()

    def merge_sstable(self):
        #TODO
        pass