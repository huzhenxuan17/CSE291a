from column import Column
from content import read_cache

READCACHE_FLAG = True
READCACHE_THRESHOLD = 1000

class Table(object):
    """docstring for table"""

    def __init__(self):
        self.columnlist = dict()
        self.row_keys = set()
        if READCACHE_FLAG:
            self.rc = read_cache.ReadCache(READCACHE_THRESHOLD)

    def get(self, col, row):
        if col in self.columnlist:
            if READCACHE_FLAG: # using readcache
                result = self.rc.get(col, row)
                if result:
                    return result
                else:
                    result = self.columnlist[col].get(row)
                    self.rc.set(col, row, result)
                    return result
            else:
                result = self.columnlist[col].get(row)
                return result
        else:
            return "wrong column name!"

    def get_table(self):
        table = dict()
        for col in self.columnlist:
            tmp = []
            for row in self.row_keys:
                tmp.append(self.columnlist[col].get(row))
            table[col] = tmp
        return table

    def get_col(self, col):
        column = []
        for key in self.row_keys:
            column.append(key+':'+self.columnlist[col].get(key))
        return column

    def get_row(self, row):
        rows = []
        for col in self.columnlist:
            rows.append(col+':'+self.columnlist[col].get(row))
        return rows

    def where(self, attribute, query):
        if attribute in self.columnlist:
            records = self.get_col(attribute)
        elif attribute in self.row_keys:
            records = self.get_row(attribute)
        else:
            return "attribute incorrect!"
        res = []
        for item in records:
            tmp = item.split(':')
            if sum([char.isdigit() or char=='.' for char in tmp[1]])==len(tmp[1]):
                equation = tmp[1]+query
            else:
                equation = "'"+tmp[1]+"'"+query
            if eval(equation):
                res.append(':'.join(tmp))
        return res


    def put(self, col, row, value, compresstype=0):
        '''

        :param compresstype:
            0: uncompressed
            1: string compressed
            2: block compressed
        :return:
        '''
        self.row_keys.add(row)
        if col in self.columnlist:
            if READCACHE_FLAG:
                self.rc.delete(col, row)
            return self.columnlist[col].add(row, value)
        else:
            newcol = Column(col, compresstype)
            newcol.add(row, value)
            self.columnlist[col] = newcol

    def delete(self, col, row):
        if col in self.columnlist:
            if READCACHE_FLAG:
                self.rc.delete(col, row)
            return self.columnlist[col].delete(row)
        else:
            return "wrong column name!"
