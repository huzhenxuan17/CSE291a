from db_impl import DBImpl


class db_user(object):
    """docstring for db_user"""

    def __init__(self):
        self.db = DBImpl()

    def open(self, table_name, compressType):
        self.db.creat_table(table_name, compressType)

    def select(self, table, col, row):
        return self.db.get(table, col, row)

    def delete(self, table, col, row):
        self.db.put(table, col, row, 0)

    def insert(self, table, col, row):
        self.db.put(table, col, row, 1)


if __name__ == "__main__":
    dataBase = db_user()
    dataBase.open("table1", 0)
