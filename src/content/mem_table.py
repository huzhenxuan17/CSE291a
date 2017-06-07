THRESHOLD = 4 * 1024


class MEMTable:
    def __init__(self):
        self.mem_table = {}
        self.usage = 0

    def add(self, key, value):
        if key in self.mem_table:
            self.usage = self.usage - len(key) - len(self.mem_table[key]) - 1
        self.mem_table[key] = value
        self.usage = self.usage + len(key) + len(value) + 1
        if self.usage >= THRESHOLD:
            return True
        else:
            return False

    def clear(self):
        self.mem_table = {}
        self.usage = 0
