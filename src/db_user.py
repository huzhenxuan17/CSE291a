from db_impl import DBImpl
import random


class DBUser(object):
    """docstring for db_user"""

    def __init__(self):
        self.db = DBImpl()

    def open(self, table_name, compressType):
        self.db.open_table(table_name, compressType)

    def select(self, table, col='*', row='*'):
        return self.db.get(table, col, row)

    def delete(self, table, col, row):
        self.db.delete(table, col, row)

    def insert(self, table, col, row, value):
        self.db.put(table, col, row, value)

    def where(self, table, attribute, query):
        return self.db.where(table, attribute, query)


def randomString(length, randomLength=False):
    if randomLength:
        length = random.randint(1, length)
    result = ""
    model = "abcdefghijlmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for i in range(length):
        seed = random.randint(0, len(model) - 1)
        result += model[seed]
    return result

if __name__ == "__main__":
    app = DBUser()
    table_name = 'test_select'
    app.open(table_name, 0)
    for r in range(5):
        for c in ['a', 'b', 'c']:
            #tmp = randomString(6)
            tmp = random.randint(1, 1000)
            app.insert(table_name, c, str(r), str(tmp))
            print tmp,
        print
    print app.select(table_name)
    print app.select(table_name, col='a')
    print app.select(table_name, row='1')
    print app.where(table_name, 'a', "<200")





