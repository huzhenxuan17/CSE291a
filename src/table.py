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

    def put(self, col, row, value, Type):
        if col in self.columnlist:
            return self.columnlist[col].put(row, value, Type)
        else:
            newcol = Column(col)
            newcol.put(row, value, Type)
            self.columnlist.append(newcol)
