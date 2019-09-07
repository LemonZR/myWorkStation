#!/tongweb/bin/python/python2.7/bin/python
#coding:utf8
import os
import sys
import time
import re
import sys
reload(sys)
from datetime import datetime
sys.setdefaultencoding('utf8')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True

from Utils import  dbOper,printLog,getDate
from conf import  config
loggerDate = getDate.getToday()
logger=printLog.getLogger(loggerDate,"kaquan","%s/kaquan/log/"%BASE_DIR)
historyResultFile ='%s/kaquan/result/%s.txt'%(BASE_DIR,loggerDate)
def getConnect():
    try:
        cur1,conn1 = dbOper.conn_db(config.kqzx2_0_1)
        logger.info("连接数据库1成功")
    except Exception as e:
        logger.error("连接数据库1失败")
        cur1,conn1 = None,None
    try:
        cur2, conn2 = dbOper.conn_db(config.kqzx2_0_2)
        logger.info("连接数据库2成功")
    except Exception as e:

        logger.error("连接数据库2失败")
        cur2,conn2 = None,None
    cur =[cur1,cur2]
    conn =[conn1,conn2]
    return  cur,conn

class kaQ(object):
    def __init__(self):
        self.failedUser = []
        self.trueCount =0
        self.falseCount =0
        self.cur,self.conn =getConnect()

        self.logger = logger
        if self.cur ==[None,None]:
            logger.error("初始化数据库连接失败，退出")
            sys.exit(1)


    def query(self,batch_Id,xiaodongxi):
        batch_Id =batch_Id.strip()
        result =[]

        try:

            batchId = batch_Id
            batchsql = " select * from echnmarketdb.td_def_pcard_batch  where  BATCH_ID ="+batchId
            batchhead,batchResult = dbOper.qryData(self.cur[0], batchsql)

            # 卡券信息head拼接，只拼接一次
            head=[]
            body=[]
            if 1==1:
                for j in range(0, len(batchhead)):
                    #print(str(batchhead[j]))
                    head.append(str(batchhead[j]))
            else:
                pass



            #卡券信息body拼接
            for k in range(0,len(batchResult[0])):
                #print(str(batchResult[0][k]))
                body.append(str(batchResult[0][k]))
            print('|'.join(head))
            print('|'.join(body))



        except Exception as e:
            logger.exception("Exception Logged run")
            self.falseCount += 1
            self.failedUser.append(batch_Id)
            self.logger.error(e)
            self.logger.error ("Phone:%s Failed to Query -------------XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"%batch_Id)
        return result #这个返回，，以后可能有用。

    def batchQuery(self,batch_IdFile):
        self.logger.error("Query start")
        with open(batch_IdFile,'r') as batch_IdList:
            xiaodongxi =0
            for batch_Id in batch_IdList:
                result=self.query(batch_Id,xiaodongxi)
                xiaodongxi =1


        time.sleep(0.05)
        self.logger.error("Query finished")
        self.logger.error("\n错误计数："+self.falseCount.__str__()+"\n错误号码："+self.failedUser.__str__())

        return result




if __name__== "__main__":

    run = kaQ()
    try:
        batch_Id = str(sys.argv[1]).strip()
        print "batch_Id is: "+ batch_Id
        run.query(batch_Id,0)

    except Exception as e:
        logger.error("脚本无参数执行，读取号码文件查询")