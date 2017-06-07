import random
import db_user

def randomString(length):
    result = ""
    model = "abcdefghijlmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    for i in range(length):
        seed = random.randint(0, len(model) - 1)
        result += model[seed]
    return result

def test_case1():
    dataBase = db_user.DBUser()
    dataBase.open("table1", 0)
    for i in range(1000):
        dataBase.insert("table1", "col1", randomString(2), randomString(10))
        print i


test_case1()
# test_case1()