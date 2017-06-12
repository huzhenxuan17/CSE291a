from table import Table


class DBImpl(object):
    """docstring for db_impl"""

    def __init__(self):
        self.tablelist = dict()

    def open_table(self, table_name, compressType=0):
        if table_name in self.tablelist:
            return "table already exist"
        else:
            table = Table()
            self.tablelist[table_name] = table

    def get(self, table, col, row):
        if table in self.tablelist:
            if col == '*' and row == '*':
                return self.tablelist[table].get_table()
            elif col == '*' and row != '*':
                return self.tablelist[table].get_row(row)
            elif col != '*' and row == '*':
                return self.tablelist[table].get_col(col)
            else:
                return self.tablelist[table].get(col, row)
        else:
            return "table not exist!"

    def put(self, table, col, row, value):
        if table in self.tablelist:
            return self.tablelist[table].put(col, row, value)
        else:
            return "table not exist!"

    def delete(self, table, col, row):
        if table in self.tablelist:
            return self.tablelist[table].delete(col, row)
        else:
            return "table not exist!"

    def where(self, table, attribute, query):
        if table in self.tablelist:
            return self.tablelist[table].where(attribute, query)
        else:
            return "table not exist!"

if __name__ == "__main__":
    dataBase = DBImpl()
    dataBase.open_table("table1")
    dataBase.get("table1", "col12", "row1")
