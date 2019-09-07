#coding: utf-8

from multiprocessing import Pool
import time
import random


class test(object):

    def mycallback(self,x):
        print str(x)

def test_1(i):
    print i
def sayHi(num):
    time.sleep(random.random()) #random.random()随机生成0-1之间的小数
    return num

if __name__ == '__main__':
    start = time.time()
    pool = Pool(100)
    a =test
    b =test.mycallback
    for i in range(100):
        pool.apply_async(sayHi, (i,), callback=b)
    pool.close()
    pool.join()
    stop = time.time()
    print 'delay: %.3fs' % (stop - start)