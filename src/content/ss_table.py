import bloom_filter


class SSTable:
    """
    the set of SSTable
    """
    def __init__(self, mem_dict, file_name):
        self.file_name = file_name # TODO hard code
        self.bf = bloom_filter.BloomFilter(6)
        if isinstance(mem_dict, dict):
            print "using dictionary as input"
            mem_list = []
            for key in mem_dict:
                mem_list.append((key, mem_dict[key]))
            mem_list.sort()
        else:
            mem_list = mem_dict

        # TODO save the sorted list into disk and  update bloom_filter
        fp_data = open(self.file_name+'_data.dat', 'w')
        fp_index = open(self.file_name+'_idx.dat', 'w')
        next_offset = 0
        for item in mem_list:
            self.bf.update(item[0])
            current_item = item[0] +"\t" + item[1]+"\n"
            fp_data.write(current_item)
            fp_index.write(item[0] +"\t" + str(next_offset) + '\n')
            next_offset += len(current_item)

    def get(self, key):
        fp_index = open(self.file_name + "_idx.dat", 'r')
        index = [line.split() for line in fp_index.readlines()]
        lo = 0
        hi = len(index)
        offset = -1
        while lo <= hi:
            mid = (lo+hi)//2
            if index[mid][0] == key:
                offset = int(index[mid][1])
                break
            elif index[mid][0] < key:
                lo = mid+1
            else:
                hi = mid-1
        if offset == -1:
            return None

        fp_data = open(self.file_name + "_data.dat", 'r')
        fp_data.seek(offset,0)
        data = fp_data.readline().split('\t')[1]
        return data.rstrip()

    def to_list(self):
        # TODO return a list of the file
        result = []
        with open(self.file_name) as fp:
            for line in fp:
                line_tuple = line.strip().split("\t")
                result.append((line_tuple[0], line_tuple[1]))
        return result

if __name__=="__main__":
    mem_dict = {'1': 'aaa',
                '2': 'bbbbbb',
                '3': 'ccdcd'}
    test = SSTable(mem_dict, '../data/tmp2')
    print test.get('2')
    print test.get('1')


