from content import mem_table
from content import ss_table

DELETE_FLAG = "/"
MERGE_THRESHOLD = 10


class Column:
    def __init__(self, col_name):
        self.col_name = col_name
        self.bigFile = []
        self.smallFile = []
        self.mt = mem_table.MEMTable()

    def add(self, key, value):
        value = value.replace(DELETE_FLAG, "/"+DELETE_FLAG)
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
        return result.replace("/" + DELETE_FLAG, DELETE_FLAG)

    def persistence(self):
        """
        1. save mem_table to ss_table
        2. update bloom_filter
        :return:
        """
        ss_table_dict = self.mt.mem_table
        file_name = "data/" + self.col_name + '/small' + str(len(self.smallFile)) 
        st = ss_table.SSTable(ss_table_dict, file_name) # inside : update bloom filter
        self.smallFile.append(st)
        self.mt.clear()
        
        # merge smallFile
        if len(self.smallFile) >= MERGE_THRESHOLD:
            self.merge_sstable()

    def merge_sstable(self):
        sstable_list = []
        for sstable in reversed(self.smallFile):
            sstable_list.append(sstable.to_list())
        new_sstable_list = Column.merge_list(sstable_list)
        file_name = "data/" + self.col_name + '/big' + str(len(self.bigFile)) 
        st = ss_table.SSTable(new_sstable_list, file_name)
        self.bigFile.append(st)

    @staticmethod
    def merge_list(sstable_raw):
        length = len(sstable_raw)
        if length == 1:
            return sstable_raw[0]
        list1 = Column.merge_list(ss_table[:length//2])
        list2= Column.merge_list(ss_table[length//2:])
        #TODO merge

if __name__ == "__main__":
    cc = Column()
    cc.add("ccc ", "haha")
    cc.add("aaa", "you are my sunshine")
    cc.add("bb", "my only sunshine")
    cc.add("a", "LoL")
    cc.persistence()
    ad = cc.smallFile[0].to_list()
    print ad
