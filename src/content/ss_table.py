import bloom_filter
import zlib
import platform
import gzip
import os

COMPRESS_TEST = False

class SSTable:
    """
    the set of SSTable
    """

    def __init__(self, mem_dict, file_name, compresstype):
        self.file_name = file_name
        self.compresstype = compresstype
        self.bf = bloom_filter.BloomFilter(4)
        if isinstance(mem_dict, dict):
            mem_list = []
            for key in mem_dict:
                mem_list.append((key, mem_dict[key]))
            mem_list.sort()
        else:
            mem_list = mem_dict
        if self.compresstype == 2:
            fp_data = gzip.open(self.file_name+'_data.dat.gz', 'wb')
            fp_index = gzip.open(self.file_name+'_idx.dat.gz', 'wb')
        else:
            fp_data = open(self.file_name + '_data.dat', 'w')
            fp_index = open(self.file_name + '_idx.dat', 'w')
        next_offset = 0
        for item in mem_list:
            self.bf.update(item[0])
            string = item[0] + "\t" + item[1] + "\n"
            if self.compresstype == 1:
                current_item = zlib.compress(string, 7)
            else:
                current_item = string
            fp_data.write(current_item)
            fp_index.write(item[0] + "\t" + str(next_offset) + '\n')
            next_offset += len(current_item)
            if platform.system() == "Windows" and self.compresstype == 0:
                next_offset += 1  # do it for windows
        if COMPRESS_TEST:
            if compresstype == 2:
                print "file size:", os.stat(self.file_name + "_data.dat.gz").st_size
            else:
                print "file size:", os.stat(self.file_name + "_data.dat").st_size


    def get(self, key):
        if self.compresstype == 2:
            fp_index = gzip.open(self.file_name + "_idx.dat.gz", 'rb')
        else:
            fp_index = open(self.file_name + "_idx.dat", 'r')
        index = [line.split() for line in fp_index.readlines()]
        lo = 0
        hi = len(index) -1
        offset = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            if index[mid][0] == key:
                offset = int(index[mid][1])
                break
            elif index[mid][0] < key:
                lo = mid + 1
            else:
                hi = mid - 1
        if offset == -1:
            return None

        if self.compresstype == 2:
            fp_data = gzip.open(self.file_name + "_data.dat.gz", 'rb')
        else:
            fp_data = open(self.file_name + "_data.dat", 'r')
        fp_data.seek(offset, 0)
        if self.compresstype == 1:
            data = zlib.decompress(fp_data.readline()).split('\t')[1]
        else:
            data = fp_data.readline().split('\t')[1]
        return data.rstrip()

    def to_list(self):
        result = []
        if self.compresstype == 2:
            fp = gzip.open(self.file_name + "_data.dat.gz", 'rb')
        else:
            fp = open(self.file_name + "_data.dat", 'r')
        for line in fp:
            line_tuple = line.strip().split("\t")
            result.append((line_tuple[0], line_tuple[1]))
        return result


if __name__ == "__main__":
    mem_dict = {'1': 'aaa',
                '2': 'bbbbbb',
                '3': 'ccdcd'}
    test = SSTable(mem_dict, '../data/tmp2', 2)
    print test.get('2')
    print test.get('1')
    #print test.to_list()
