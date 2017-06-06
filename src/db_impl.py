from table import Table


class DBImpl(object):
    """docstring for db_impl"""

    def __init__(self):
        self.tablelist = dict()

    def creat_table(self, table_name, compressType):
        if table_name in self.tablelist:
            return "table already exist"
        else:
            table = Table()
            self.tablelist[table_name] = table

    def get(self, table, col, row):
        if table in self.tablelist:
            return self.tablelist[table].get(col, row)
        else:
            return "table not exist!"

    def put(self, table, col, row, value, Type):
        if table in self.tablelist:
            return self.tablelist[table].put(col, row, value, Type)
        else:
            return "table not exist!"
