from content import mem_table
from content import ss_table
import os

DELETE_FLAG = "/"
MERGE_THRESHOLD = 10
BLOOMFILTER_FLAG = True

class Column:
    def __init__(self, col_name, compresstype):
        self.col_name = col_name
        self.compresstype = compresstype
        self.bigFile = []
        self.smallFile = []
        self.mt = mem_table.MEMTable()
        if not os.path.isdir("data/"+col_name):
            os.mkdir("data/"+col_name)

    def add(self, key, value):
        value = value.replace(DELETE_FLAG, "/" + DELETE_FLAG)
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
                if (not BLOOMFILTER_FLAG) or sstable.bf.check(key):
                    result = sstable.get(key)
                    if result is not None:
                        break
        if not result:
            for sstable in reversed(self.bigFile):
                if (not BLOOMFILTER_FLAG) or sstable.bf.check(key):
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
        st = ss_table.SSTable(ss_table_dict, file_name, self.compresstype)  # inside : update bloom filter
        self.smallFile.append(st)
        self.mt.clear()
        # merge smallFile
        if len(self.smallFile) >= MERGE_THRESHOLD:
            self.merge_sstable()

    def merge_sstable(self):
        sstable_list = []
        for sstable in self.smallFile:
            sstable_list.append(sstable.to_list())
        new_sstable_list = Column.merge_list(sstable_list)
        file_name = "data/" + self.col_name + '/big' + str(len(self.bigFile))
        st = ss_table.SSTable(new_sstable_list, file_name, self.compresstype)
        self.bigFile.append(st)

        # remove all small File
        for sstable in self.smallFile:
            path1 = sstable.file_name + "_idx.dat"
            path2 = sstable.file_name + "_data.dat"
            os.remove(path1)
            os.remove(path2)
        self.smallFile = []

    @staticmethod
    def merge_list(sstable_raw):
        length = len(sstable_raw)
        if length == 1:
            return sstable_raw[0]
        list1 = Column.merge_list(sstable_raw[:length // 2])
        list2 = Column.merge_list(sstable_raw[length // 2:])
        result = []
        len1 = len(list1)
        len2 = len(list2)
        i1 = 0
        i2 = 0
        while i1 < len1 and i2 < len2:
            if list1[i1][0] == list2[i2][0]:
                # list2 is newer than list1
                result.append(list2[i2])
                i1 += 1
                i2 += 1
            elif list1[i1][0] < list2[i2][0]:
                result.append(list1[i1])
                i1 += 1
            else:
                result.append(list2[i2])
                i2 += 1
        if i1 < len1:
            result.extend(list1[i1:])
        elif i2 < len2:
            result.extend(list2[i2:])
        return result


if __name__ == "__main__":
    # cc = Column("test")
    # cc.add("ccc ", "haha")
    # cc.add("aaa", "you are my sunshine")
    # cc.add("bb", "my only sunshine")
    # cc.add("a", "LoL")
    # cc.persistence()
    # ad = cc.smallFile[0].to_list()
    # print ad

    """
    test merge
    """
    # list1 = [("a", "123"), ("b", "123"), ("d", "000"),("f","222")]
    # list2 = [("a", "223"), ("bb", "444"), ("c", "555"), ("d", "666")]
    # result = []
    # len1 = len(list1)
    # len2 = len(list2)
    # i1 = 0
    # i2 = 0
    # while i1 < len1 and i2 < len2:
    #     if list1[i1][0] == list2[i2][0]:
    #         # list2 is newer than list1
    #         result.append(list2[i2])
    #         i1 += 1
    #         i2 += 1
    #     elif list1[i1][0] < list2[i2][0]:
    #         result.append(list1[i1])
    #         i1 += 1
    #     else:
    #         result.append(list2[i2])
    #         i2 += 1
    # if i1 < len1:
    #     result.extend(list1[i1:])
    # elif i2 < len2:
    #     result.extend(list2[i2:])
    # print result
