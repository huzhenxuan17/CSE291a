from bitarray import bitarray
import hashlib


class BloomFilter:
    def __init__(self, digit):
        self.valid_digit = digit  # range from 6 to 8
        self.array_size = 16 ** self.valid_digit
        self.bitArray = bitarray(self.array_size)
        self.bitArray.setall(False)

    def check(self, key):
        v1, v2, v3 = self.hash_functions(key)
        if self.bitArray[v1] and self.bitArray[v2] and self.bitArray[v3]:
            return True
        return False

    def update(self, key):
        v1, v2, v3 = self.hash_functions(key)
        self.bitArray[v1] = True
        self.bitArray[v2] = True
        self.bitArray[v3] = True

    def hash_functions(self, key):
        hash1 = hashlib.sha224(key)
        hash2 = hashlib.sha256(key)
        hash3 = hashlib.sha1(key)
        v1 = int(hash1.hexdigest()[-self.valid_digit:], 16)
        v2 = int(hash2.hexdigest()[-self.valid_digit:], 16)
        v3 = int(hash3.hexdigest()[-self.valid_digit:], 16)
        return v1, v2, v3

if __name__ == "__main__":
    pass
    # bf = BloomFilter()
    # res1 = bf.hash_functions("you are my sunshine")
    # print res1
    #
    # bf.update("ilovea")
    # bf.update("iloveb")
    # bf.update("ilovec")
    #
    # print bf.check("ilovec")
    # print bf.check("iloved")
    # print bf.check("ilovee")

    # bt = bitarray(50)
    # bt.setall(False)
    # bt[20] = True
    # for i in range(44):
    #     print bt[i] , i
