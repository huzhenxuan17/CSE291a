class ReadCache:
    def __init__(self, max_cache=10):
        self.max_cache = max_cache
        self.head = Node("-1", "-1")
        self.tail = Node("1", "1")
        self.head.post = self.tail
        self.tail.pre = self.head
        self.map = {}

    def get(self, column_key, row_key):
        key = column_key + "_" + row_key
        if key in self.map:
            result = self.delete_node(key).value
            self.add_node_to_head(key, result)
            return result
        return None

    def set(self, column_key, row_key, value):
        key = column_key + "_" + row_key
        if key in self.map:
            self.delete_node(key)
        self.add_node_to_head(key, value)
        if len(self.map) > self.max_cache:
            self.delete_node(self.tail.pre.key)

    def delete(self, column_key, row_key):
        key = column_key + "_" + row_key
        if key in self.map:
            return self.delete_node(key)
        return None

    def delete_node(self, key):
        if key in self.map:
            node = self.map[key]
            node.pre.post = node.post
            node.post.pre = node.pre
            return self.map.pop(key)
        return None

    def add_node_to_head(self, key, value):
        if key not in self.map:
            node = Node(key, value)
            node.pre = self.head
            node.post = self.head.post
            self.head.post.pre = node
            self.head.post = node
            self.map[key] = node

    def to_string(self):
        for key in self.map:
            print key, self.map[key].value


class Node:
    def __init__(self, key=None, value=None):
        self.key = key
        self.value = value
        self.pre = None
        self.post = None


if __name__ == "__main__":
    rc = ReadCache()
    for i in range(20):
        rc.set("col1", str(i), "haha" + str(i))
    print rc.get("col1", "4")
    print rc.get("col1", "15")
    for i in range(100, 109):
        rc.set("col1", str(i), "haha" + str(i))
    print "-----"
    rc.to_string()
