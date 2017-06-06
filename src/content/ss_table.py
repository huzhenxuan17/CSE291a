class SSTable():
    """
    the set of SSTable
    """
    def __init__(self, mem_dict, file_name):
        self.file_name = file_name # TODO hard code
        if isinstance(mem_dict, dict):
            print "using dictionary as input"
            mem_list = []
            for key in mem_dict:
                mem_list.append((key,mem_dict[key]))
            mem_list.sort()
        else:
            mem_list = mem_dict

        # TODO save the sorted list into disk and  update bloom_filter
        fp = open(self.file_name, 'w')
        for item in mem_list:
            fp.write(item[0] +"\t" + item[1]+"\n")


    def get(self, key):
        # TODO return None if not found, return value if found
        pass

    def to_list(self):
        # TODO return a list of the file
        result = []
        with open(self.file_name) as fp:
            for line in fp:
                line_tuple = line.strip().split("\t")
                result.append((line_tuple[0], line_tuple[1]))
        return result
