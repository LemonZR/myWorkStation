#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import calendar
import random
import sys
import time
from configModule import getconfig
from ftpModule import sftpTools
from toolsModule import commTools
import logging
import logging.config


def createFile():
    fpa = open("E:\\busi.dat","wb")
    fpb = open("E:\\coupons.dat","wb")
    for i in range(10000000):
        line = "20180117|%s|%s|%s|351|10\n" % (getorderid(),getrandom(),getPhone())
        fpa.write(line)
        fpb.write(line)
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpa [%s]\n" % (line)
    fpa.write(line)
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpa [%s]\n" % (line)
    fpa.write(line)
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpa [%s]\n" % (line)
    fpa.write(line)
    fpa.close()
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpb [%s]\n" % (line)
    fpb.write(line)
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpb [%s]\n" % (line)
    fpb.write(line)
    line = "20180117|%s|%s|%s|351|10\n" % (getorderid(), getrandom(), getPhone())
    print "debug fpb [%s]\n" % (line)
    fpb.write(line)
    fpb.close()

def getab(a, b):
    return a + b


def getbeginenddate(mydate):
    c = getab(1, 2)
    t = time.strptime(mydate, "%Y%m")
    begin = time.strftime("%Y-%m", t) + "-01"
    end = time.strftime("%Y-%m", t) + "-" + str(calendar.monthrange(t[0], t[1])[1])
    return begin, end


def writefile():
    begin = time.time()
    fp = open("C:\\Users\\Ivan\\Desktop\\mytest.log", "wb")
    for i in range(1000000):
        fp.write("%s\n" % (getrandom()))
    fp.close()
    end = time.time()
    print end - begin


def getrandom():
    return ''.join(random.sample(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'
                                     , 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x'
                                     , 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                 16)).replace(" ", "")

def getPhone():
    haoduan = ''.join(random.sample(['138','139','136','135','186','135'],1))
    weihao = ''.join(random.sample(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], 8))
    return haoduan+weihao

def getorderid():
    return ''.join(random.sample(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L'
                                     , 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X'
                                     , 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], 20))
def testret(num):
    if num == 0:
        return 1,2,3
    else:
        return False

def testcmp():
    mylist = []
    for i in range(1000000):
        mylist.append(getrandom())
    print sys.getsizeof(mylist)
    print len(mylist)
    a1 = getrandom()
    a2 = getrandom()
    a3 = getrandom()
    print a1 + "..." + a2 + "..." + a3
    b1 = getrandom()
    b2 = getrandom()
    b3 = getrandom()
    print b1 + "..." + b2 + "..." + b3
    blist = mylist[:]
    mylist.append(a1)
    mylist.append(a2)
    mylist.append(a3)
    blist.append(b1)
    blist.append(b2)
    blist.append(b3)
    begin = time.time()
    # mylist.sort()
    # blist.sort()
    print list(set(mylist).difference(set(blist)))
    end = time.time()

    print mylist[665]
    print end - begin

    name = input("Please intput your name:")

def strproc(str):
    return str[0].upper() + str[1:].lower()

def testsftp():
    host = '127.0.0.1'  # 主机
    port = 8822  # 端口
    username = 'e3base'  # 用户名
    password = "XHyC8qKrpRqN"  # 密码
    local = 'E:\\sftptest\\ruame.ini'  # 本地文件或目录，与远程一致，当前为windows目录格式，window目录中间需要使用双斜线
    remote = '/e3base/work/lpc/'  # 远程文件或目录，与本地一致，当前为linux目录格式
    sftpTools.sftp_upload(host, port, username, password, local, remote)  # 上传

def testlist():
    listA = ['a','b','c','a']
    list1 = ['20180226', 'sdjakjsdf', 'a']
    list2 = ['20180223', 'xcvxcbcvb', 'b']
    list3 = ['20180226', 'tuiyityui', 'c']
    list4 = ['20180229', 'erhgfjgjj', 'a']
    listB = {}

    if not listB.has_key('a'):
        listB['a'] = []
    listB['a'].append(list1)
    if not listB.has_key('b'):
        listB['b'] = []
    listB['b'].append(list2)
    if not listB.has_key('c'):
        listB['c'] = []
    listB['c'].append(list3)
    if not listB.has_key('a'):
        listB['a'] = []
    listB['a'].append(list4)

    print listB


if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")
    print getconfig.readsftpinfo('recharge_sftp')
    print commTools.gettimestr()
    listA = ['a', 'b', 'c', 'a']
    a, b, c, d = listA
    print a
    print b
    print c
    #testlist()
    #logger = logging.getLogger("shareLimitList")
    #logger.debug("test")
    #strlist = ['adam', 'LISA', 'barT']
    #print map(lambda str: str[0].upper() + str[1:].lower(), strlist)
    #begin = time.time()
    #createFile()
    #fileCmp.cmpdiff("E:\\busi.dat", "E:\\coupons.dat")
    #fileCmp.readfile1()
    #print fileCmp.filepathcheck("E:\\abc\\dsk\\account_recharge2coupons_20180118_351_001.dat")
    #a = [1, 2, 3, 4]
    #print "|".join(str(i) for i in a)
    #end = time.time()
    #print end - begin
    #name = input("Please intput your name:")
    #print name
    #print getPhone()
    #dblist =  getconfig.readdblist()
    #print dblist
    #for dbinfo in dblist:
    #    print getconfig.readdbinfo(dbinfo[0]).split(',')
    # writefile()
    # print os.path.getsize("C:\\Users\\Ivan\\Desktop\\a_YDSC1-R1.11-11101_201712_00_001.dat")
    # print getbeginenddate("201708")
    # print getrandom()
