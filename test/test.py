# coding:utf8


import os
import time
import sys
import _md5
#########
# python2 需要对编码进行处理
reload(sys)
sys.setdefaultencoding('utf8')
###########
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import dbOper, printLog, getDate
from conf import config
loggerDate = getDate.getToday()
logger = printLog.getLogger(loggerDate, "kaquan", "%s/kaquan/log/" % BASE_DIR)



class kaQ(object):
    def __init__(self):
        self.count = 0

        self.logger = logger


    def query(self, ):
        result = []
        resDict={}
        historyResultFile = '%s/obtain_%s.xlsx' % (BASE_DIR, loggerDate)
        historyResult = open(historyResultFile, "a")
        for cur in ("XiaoMing","Lilei","Danny"):
            for databaseNum in ('a','b', 'c'):  # range(1,6)---> 序号1到5的数据库
                for phoneTail in  ('d', 'e'):  # 号码尾号0-9
                    resultbody = []
                    try:
                        qryHead, qryResult = ["phoneTail","count1","count2"],[[cur,databaseNum + '1',phoneTail + '1'],[cur,databaseNum + '2',phoneTail + '2'],[cur,databaseNum + '3' ,phoneTail + '3']]
                        print qryResult
                        if self.count == 0:
                            resulthead = []
                            for j in range(0, len(qryHead)):
                                resulthead.append(str(qryHead[j]))
                            result.append('|'.join(resulthead))  # 首先写入头信息
                            historyResult.write(','.join(resulthead) + "\n")
                        self.count += len(qryResult)
                        for i in range(0, qryResult.__len__()):

                                resDict[qryResult[i][0]]=map(lambda (a,b):a+b,zip(resDict.get(qryResult[i][0],['','']),[qryResult[i][1],qryResult[i][2]]))
                                #print resDict
                    except Exception as e:
                        self.logger.error(e)
        for k,v in resDict.items():
            print k,v[0],v[1]
            #result.append('|'.join([k,v[0],v[1]]))
            #historyResult.write('|'.join([k,v[0],v[1]]) + "\n")
        historyResult.close()
        time.sleep(0.05)
        self.logger.error("Query finished")
        logger.error("计数：" + str(self.count))

        return result


if __name__ == "__main__":
    a=kaQ()
    a.query()