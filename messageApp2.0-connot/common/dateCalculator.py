#!/e3base/python/Python-2.7.6/python
#coding:utf8

import os
import sys
import time,datetime
from dateutil.relativedelta import relativedelta
from myException import myException
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True


def dayAdd(dayStr,count):
    try:
        timeStruct = datetime.datetime.strptime(dayStr, "%Y%m%d")
    except:
        try:
            timeStruct = datetime.datetime.strptime(dayStr, "%Y-%m-%d")
        except Exception as e:
            raise myException(e,sys.argv[0],sys._getframe().f_back.f_lineno)

    try:
        resultDate = (timeStruct+ datetime.timedelta(int(count))).strftime('%Y%m%d')
        return resultDate
    except Exception as e:
        raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)

    #today = time.strftime('%Y%m%d', time.localtime(time.time()))

def monthAdd(dayStr,count):
    try:
        timeStruct = datetime.datetime.strptime(dayStr, "%Y%m%d")
    except:
        try:
            timeStruct = datetime.datetime.strptime(dayStr, "%Y-%m-%d")
        except Exception as e:
            raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
    try:
        resultDate = (timeStruct + relativedelta(months=int(count))).strftime('%Y-%m-%d')

        return resultDate
    except Exception as e:
        raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)

if __name__=='__main__':
    try:
        monthAdd("s","s")
    except Exception as e:
        print e