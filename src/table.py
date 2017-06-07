from column import Column


class Table(object):
    """docstring for table"""

    def __init__(self):
        self.columnlist = dict()

    def get(self, col, row):
        if col in self.columnlist:
            return self.columnlist[col].get(row)
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
            return self.columnlist[col].add(row, value)
        else:
            newcol = Column(col, compresstype)
            newcol.add(row, value)
            self.columnlist[col] = newcol
