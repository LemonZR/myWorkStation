#coding:utf8

import threading
import time


def sayHi(num,f):

    f.write(num)


if __name__ == '__main__':
    e1 = time.time()

    f= open('123.txt',"a")
    threads=[]
    for i in range(0, 100):
        tName = ('%06d' % i).__str__()

        t = threading.Thread(target=sayHi ,name=tName, args=(tName, f))
        threads.append(t)
    for thread in threads:
        thread.setDaemon(True)
        print thread.getName()
        thread.start()

    for thread in threads:
        thread.join()