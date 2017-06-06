class db_impl(object):
	"""docstring for db_impl"""
	def __init__(self):
		self.tablelist = dict()

	def creat_table(self, table_name, compressType):
		if table_name in tablelist:
			return "table already exist"
		else:
			table = new Table()
			self.tablelist[table_name]=table

	def get(self, table, col, row):
		if table in self.tablelist:
			return self.tablelist[table].get(col, row)
		else:
			return "table not exist!"

	def put(self, table, col, row, value, Type):
		if table in self.tablelist:
			return self.tablelist[table].put(col, row, value,Type)
		else:
			return "table not exist!"


class table(object):
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
			newcol = new mem_table()
			newcol.put(row, value, Type)
			columnlist.append(newcol)

