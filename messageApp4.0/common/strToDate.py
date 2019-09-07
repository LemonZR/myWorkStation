import os
import sys

import time,datetime
from myException import myException
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True



class myDate(object):
    def __init__(self):
        self.nowDay = time.strftime('%d', time.localtime(time.time()))
        self.nowMonth = time.strftime('%m', time.localtime(time.time()))
        self.nowYear = time.strftime('%Y', time.localtime(time.time()))
        self.yesDay = (datetime.datetime.today() + datetime.timedelta(-1)).strftime('%d')
        self.yesMon = (datetime.datetime.today() + datetime.timedelta(-1)).strftime('%m')
        self.yesYear = (datetime.datetime.today() + datetime.timedelta(-1)).strftime('%Y')
        self.yesterday=(datetime.datetime.today() + datetime.timedelta(-1)).strftime('%Y%m%d')
        self.today=time.strftime('%Y%m%d', time.localtime(time.time()))

    def toDate(self,fmt,dayStr,fromFmt='%Y%m%d%H%M%S"'):
        try:
            timeStruct = time.strptime(dayStr, "%Y%m%d")
        except:
            try:
                timeStruct = time.strptime(dayStr, "%Y-%m-%d")

            except :
                try:
                    timeStruct = time.strptime(dayStr, "%Y-%m-%d %H:%M:%S")
                except:
                    try:
                        timeStruct = time.strptime(dayStr, "%Y/%m/%d %H:%M:%S")
                    except:
                        try:
                            timeStruct = time.strptime(dayStr, fromFmt)
                        except Exception as e:
                            raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
        try:
            resultDate = time.strftime(fmt, timeStruct)

            return resultDate
        except Exception as e:
            raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
    def toYm(self,dayStr):
        return self.toDate('%Y%m',dayStr)
    def toY(self,daStr):
        return self.toDate("%Y",daStr)
if __name__=="__main__":
    mydate=myDate()
    myDate.toDate("%Y%m%d",'20180901')