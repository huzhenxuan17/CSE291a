from bitarray import bitarray
import hashlib

VALID_DIGIT = 6 # range from 6 to 8
ARRAY_SIZE = 16 ** VALID_DIGIT


class BloomFilter:
    def __init__(self):
        self.bitArray = bitarray(ARRAY_SIZE)
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
        pass

    @staticmethod
    def hash_functions(key):
        hash1 = hashlib.sha224(key)
        hash2 = hashlib.sha256(key)
        hash3 = hashlib.sha1(key)
        v1 = int(hash1.hexdigest()[-VALID_DIGIT:], 16)
        v2 = int(hash2.hexdigest()[-VALID_DIGIT:], 16)
        v3 = int(hash3.hexdigest()[-VALID_DIGIT:], 16)
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
