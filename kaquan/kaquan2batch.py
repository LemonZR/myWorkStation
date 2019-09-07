#coding:utf8
import os
import time
import sys
#########
#python2 需要对编码进行处理
reload(sys)
sys.setdefaultencoding('utf8')
###########
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import  dbOper,printLog,getDate
from conf import  config
loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"kaquan","%s/kaquan/log/"%BASE_DIR)
historyResultFile ='%s/kaquan/result/%sBATCH.csv'%(BASE_DIR,loggerDate)
def getConnect():
    try:
        cur1,conn1 = dbOper.conn_db_new(config.local1_new)
        logger.info("连接数据库1成功")
    except Exception as e:
        logger.info(e)
        logger.error("连接数据库1失败")
        cur1,conn1 = None,None
    try:
        cur2, conn2 = dbOper.conn_db_new(config.local2_new)
        logger.info("连接数据库2成功")
    except Exception as e:
        logger.info(e)
        logger.error("连接数据库2失败")
        cur2,conn2 = None,None
    cur =[cur1,cur2]
    conn =[conn1,conn2]
    return  cur,conn

class kaQ(object):
    def __init__(self):
        self.count =0
        self.cur,self.conn =getConnect()
        self.logger = logger
        if self.cur ==[None,None]:
            logger.error("初始化数据库连接失败，退出")
            sys.exit(1)


    def query(self,batch_id ):
        result =[]
        historyResult = open(historyResultFile, "a")  # 记录查询历史的文件history，也可以用来存输出结果。
        for cur in self.cur:
            for databaseNum in range(1,6):#range(1,6)---> 序号1到5的数据库
                for phoneTail in range(0,10):#号码尾号0-9
                    sql = """SELECT  BIND_NO 领取号码,BIND_TIME 领取时间,BIND_AREA 省份, use_date 账期,
                            target_no
                            受赠账号, LIFECYCLE_ST
                            当前状态, USE_TIME
                            使用时间, USE_ORDER_ID
                            使用订单 ,PCARD_CASH
                            面额
                            from  echnmarketdb%d.td_pcard_info_%s 
                            where BATCH_ID in (%s) and LIFECYCLE_ST = '9' and BIND_TIME>='2018-07-01 00:00:00' """ % (databaseNum,phoneTail,batch_id)
                    logger.info(sql)
                    #and LIFECYCLE_ST = '10'
                    #print sql
                    try:
                        qryHead,qryResult = dbOper.qryData(cur, sql)
                        resultbody = []
                        if self.count==0:
                            resulthead =[]
                            for j in range(0,len(qryHead)):
                                resulthead.append(str(qryHead[j]))
                            result.append('|'.join(resulthead))  # 首先写入头信息
                            historyResult.write(','.join(resulthead) + "\n")
                        self.count += len(qryResult)
                        for i in range(0,qryResult.__len__()):
                            #查询结果
                            resultbody.append([]) #长度+1
                            for ii in range(0,len(qryResult[i])):
                                resultbody[i].append("'"+str(qryResult[i][ii])+"'")
                    except Exception as e:
                        self.logger.error(e)
                    for i in range(0,len(resultbody)):
                        #print('|'.join(resultbody[i]))#打印查询结果到控制台，logger.error 会打印时间戳，看起来乱
                        result.append('|'.join(resultbody[i]))
                        historyResult.write(','.join(resultbody[i])+"\n")
        time.sleep(0.05)
        self.logger.error("Query finished")
        logger.error("计数：" + str(self.count))
        historyResult.close()
        return result


if __name__== "__main__":

    run = kaQ()

    batch_id='20180525102624617'
    run.query(batch_id)

