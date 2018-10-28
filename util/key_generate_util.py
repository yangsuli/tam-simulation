import abc
import bisect
import math
import random
import sys


class KeyGenerator:
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._records = {}

    @staticmethod
    def create_key_generator(dist, args):
        if dist == 'zipf':
            return ZipfGenerator(*args)
        elif dist == 'random':
            return RandomGenerator(*args)
        elif dist == 'token_weight':
            return TokenWeightGenerator(*args)
        else:
            # default is increment
            return IncrementGenerator()

    def _record_key(self, key):
        if key not in self._records.keys():
            self._records[key] = 0
        self._records[key] += 1

    @abc.abstractmethod
    def next(self):
        return


class ZipfGenerator(KeyGenerator):
    def __init__(self, n, alpha):
        super(ZipfGenerator, self).__init__()
        # Calculate Zeta values from 1 to n: 
        tmp = [1. / (math.pow(float(i), alpha)) for i in range(1, n + 1)]
        zeta = reduce(lambda sums, x: sums + [sums[-1] + x], tmp, [0])

        # Store the translation map: 
        self.distMap = [x / zeta[-1] for x in zeta]

    def next(self):
        # Take a uniform 0-1 pseudo-random value: 
        u = random.random()
        # Translate the Zipf variable:
        key = bisect.bisect(self.distMap, u) - 1
        self._record_key(key)
        # print self._records
        return key


class RandomGenerator(KeyGenerator):
    def __init__(self, n):
        super(RandomGenerator, self).__init__()
        self.max_val = n

    def next(self):
        return random.randint(0, self.max_val - 1)


class IncrementGenerator(KeyGenerator):
    def __init__(self):
        super(IncrementGenerator, self).__init__()
        self.max_val = sys.maxint
        self.cur_val = 0

    def next(self):
        self.cur_val = (self.cur_val + 1) % self.max_val
        return self.cur_val


class TokenWeightGenerator(KeyGenerator):
    def __init__(self, node_token_list):
        super(TokenWeightGenerator, self).__init__()
        self.node_token_list = node_token_list
        self.helper_list = []
        for i in range(len(node_token_list)):
            for j in range(node_token_list[i]):
                self.helper_list.append(i)
        print 'token_weight_helper_list:' + str(self.helper_list)

    def next(self):
        idx = self.helper_list[random.randint(0, len(self.helper_list) - 1)]
        # print 'return next is ' + str(idx)
        return idx


def test_main():
    zp = ZipfGenerator(100, 0.99)
    freq_dict = {}
    result = []
    for i in range(1000):
        result.append(zp.next())
        # print result[i]
        if result[i] not in freq_dict.keys():
            freq_dict[result[i]] = 1
        else:
            freq_dict[result[i]] += 1

    print freq_dict


if __name__ == '__main__':
    test_main()
