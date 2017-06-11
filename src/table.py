from column import Column
from content import read_cache

READCACHE_FLAG = True
READCACHE_THRESHOLD = 1000

class Table(object):
    """docstring for table"""

    def __init__(self):
        self.columnlist = dict()
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

    def put(self, col, row, value, compresstype=0):
        '''

        :param compresstype:
            0: uncompressed
            1: string compressed
            2: block compressed
        :return:
        '''
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
