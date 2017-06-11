import random
import db_user
from content import mem_table
import column


def randomString(length, randomLength=False):
    if randomLength:
        length = random.randint(1, length)
    result = ""
    model = "abcdefghijlmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for i in range(length):
        seed = random.randint(0, len(model) - 1)
        result += model[seed]
    return result


def test_case0():
    database.open("table1", 0)
    database.insert("table1", "col0", "John", "software developer")
    database.insert("table1", "col0", "Mary", "journalist")
    database.insert("table1", "col0", "Mike", "architecture")
    print "Mary is a", database.select("table1", "col0", "Mary")
    print "John is a", database.select("table1", "col0", "John")
    database.delete("table1", "col0", "Mary")
    print "delete Mary"
    print "Mary is a", database.select("table1", "col0", "Mary")
    print "John is a", database.select("table1", "col0", "John")


def test_case1_write():
    database.open("table1", 0)
    for i in range(10000):
        database.insert("table1", "col1", randomString(2), randomString(20, randomLength=True))


def test_case1_read():
    database.open("table1", 0)
    for i in range(100):
        aaa = randomString(2)
        print database.select("table1", "col1", aaa)


def test_case1_write_overhead():
    database.open("table1", 0)
    for i in range(10000):
        a = randomString(2),
        b = randomString(20, randomLength=True)


def test_case2_write():

    # modify parameter
    mem_table.THRESHOLD = 64 * 1024
    column.MERGE_THRESHOLD = 20

    columns = ["col2_1", "col2_2", "col2_3"]
    database.open("table1", 0)
    for i in range(10000):
        database.insert("table1", columns[random.randint(0,2)], randomString(2), randomString(1000, randomLength=True))


def test_case2_read():
    database.open("table1", 0)
    for i in range(100):
        aaa = randomString(2)
        print database.select("table1", "col2_1", aaa), database.select("table1", "col2_2", aaa), database.select(
            "table1", "col2_3", aaa)


database = db_user.DBUser()
test_case0()
# test_case1_write()
# test_case1_read()
# test_case2_write()
# test_case2_read()
