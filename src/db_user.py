from db_impl import *

class db_user(object):
	"""docstring for db_user"""
	def __init__(self):
		self.db = new dB()

	def open(self, table_name, compressType):
		self.db.creat_table(table_name, compressType)

	def select(self, table, col, row):
		return self.db.get(table, col, row)

	def delete(self, table, col, row):
		self.db.put(table, col, row, 0)

	def insert(self, talbe, col, row)
		self.db.put(table, col, row, 1)
	
if __init__=="__main__":
	dataBase = new db_user()
	dataBase.open("table1", 0)
	