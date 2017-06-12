import random
import db_user
from content import mem_table
import column
import time
import table


def randomString(length, randomLength=False):
    if randomLength:
        length = random.randint(1, length)
    result = ""
    model = "abcdefghijlmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for i in range(length):
        seed = random.randint(0, len(model) - 1)
        result += model[seed]
    return result


def test_case0(compress):
    database.open("table1", compress)
    database.insert("table1", "col0", "John", "software developer")
    database.insert("table1", "col0", "Mary", "journalist")
    database.insert("table1", "col0", "Mike", "architecture")
    print "Mary is a", database.select("table1", "col0", "Mary")
    print "John is a", database.select("table1", "col0", "John")
    database.delete("table1", "col0", "Mary")
    print "delete Mary"
    print "Mary is a", database.select("table1", "col0", "Mary")
    print "John is a", database.select("table1", "col0", "John")


def test_case1_write(compress):
    start = time.time()
    database.open("table1", compress)
    for i in range(10000):
        database.insert("table1", "col1", randomString(2), randomString(20, randomLength=True))

    total_time = time.time() - start
    overhead = test_case1_write_overhead(compress)
    print "total time of test1_write:", total_time - overhead


def test_case1_read(compress):
    start = time.time()
    database.open("table1", compress)
    for i in range(1000):
        aaa = randomString(2)
        bbb = database.select("table1", "col1", aaa)
    print "total time of test1_read:", time.time() - start


def test_case1_write_overhead(compress):
    start = time.time()
    database.open("table1", compress)
    for i in range(10000):
        a = randomString(2),
        b = randomString(20, randomLength=True)
    return time.time() - start


def test_case2_write(compress):
    # modify parameter
    mem_table.THRESHOLD = 64 * 1024
    column.MERGE_THRESHOLD = 20
    columns = ["col2_1", "col2_2", "col2_3"]
    start = time.time()
    database.open("table1", compress)
    for i in range(10000):
        database.insert("table1", columns[random.randint(0, 2)], randomString(2), randomString(1000, randomLength=True))

    total_time = time.time() - start
    overhead = test_case2_write_overhead(compress)
    print "total time of test2_write:", total_time - overhead


def test_case2_read(compress):
    database.open("table1", compress)
    start = time.time()
    for i in range(1000):
        aaa = randomString(2)
        bbb = database.select("table1", "col2_1", aaa)
        ccc = database.select("table1", "col2_2", aaa)
        ddd = database.select("table1", "col2_3", aaa)
    print "total time of test2_read:", time.time() - start


def test_case2_write_overhead(compress):
    start = time.time()
    database.open("table1", compress)
    for i in range(10000):
        a = randomString(2),
        b = randomString(1000, randomLength=True)
    return time.time() - start


def test_readcache(compress):
    test_case1_write()
    database.open("table1", compress)
    start = time.time()
    # without cache
    table.READCACHE_FLAG = False
    for i in range(1000):
        aaa = "A" + randomString(1)
        bbb = database.select("table1", "col1", aaa)
    end1 = time.time()

    # without cache
    table.READCACHE_FLAG = True
    for i in range(1000):
        aaa = "A" + randomString(1)
        bbb = database.select("table1", "col1", aaa)
    end2 = time.time()

    print "without cache:", end1 - start
    print "with cache:", end2 - end1


def test_bloomfilter(compress):
    test_case1_write()
    database.open("table1", compress)
    start = time.time()
    # without cache
    column.BLOOMFILTER_FLAG = False
    for i in range(1000):
        aaa = randomString(2)
        bbb = database.select("table1", "col1", aaa)
    end1 = time.time()

    # with bloom filter
    column.BLOOMFILTER_FLAG = True
    for i in range(1000):
        aaa = randomString(2)
        bbb =  database.select("table1", "col1", aaa)
    end2 = time.time()

    print "without bloom filter:", end1 - start
    print "with bloom filter:", end2 - end1


def test_compress():
    print "---------test case 0----------"
    test_case0(0)
    print "------------------------------"
    test_case0(1)
    print "------------------------------"
    test_case0(2)
    print "---------test case1 write----------"
    test_case1_write(0)
    test_case1_write(1)
    test_case1_write(2)
    print "---------test case1 read----------"
    test_case1_read(0)
    test_case1_read(1)
    test_case1_read(2)
    print "---------test case2 write----------"
    test_case2_write(0)
    test_case2_write(1)
    test_case2_write(2)
    print "---------test case2 write----------"
    test_case2_read(0)
    test_case2_read(1)
    test_case2_read(2)


database = db_user.DBUser()


"""
workload test
"""
# test_case0(0)
# test_case1_write(0)
# test_case1_read(0)
# test_case2_write(0)
# test_case2_read(0)

"""
performance test
"""
# test_bloomfilter(0)
# test_readcache(0)
test_compress()
