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
historyResultFile ='%s/kaquan/result/%skami.csv'%(BASE_DIR,loggerDate)
def getConnect():
    try:
        cur1,conn1 = dbOper.conn_db_new(config.local1_new)
        logger.info("连接数据库1成功")
    except Exception as e:
        logger.error("连接数据库1失败")
        cur1,conn1 = None,None
    try:
        cur2, conn2 = dbOper.conn_db_new(config.local2_new)
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


    def query(self,phone,kami,xiaodongxi):
        phone =phone.strip()
        result =[]
        if re.match(r'1\d{10}$',phone):
            self.logger.info ("我是手机号")
            module = int(phone[6:7])
            databaseNum = module%5 + 1
            tableNum =(phone[-1])

            if  module <= 4 :
                cur =self.cur[0]
            else:
                cur =self.cur[1]

            sql = "select EXPIRE_TIME,BIND_NO as '领券号码' ,BIND_TIME as '领取时间', PCARD_PASSWD as '卡密' ,LIFECYCLE_ST as '状态' ,LOCK_TIME AS  '锁定时间',USE_TIME as '使用时间',USE_ORDER_ID as '充值订单号' from echnmarketdb%d.td_pcard_info_%s where bind_no ='%s' and PCARD_PASSWD=%s" % (databaseNum, tableNum, phone,kami)
            #logger.error(sql)
            # sql = "select BIND_NO, PCARD_PASSWD ,LIFECYCLE_ST ,BIND_TIME,USE_TIME,USE_DATE,TARGET_NO from echnmarketdb%d.td_pcard_info_%s where bind_no ='%s'" % (databaseNum, tableNum, phone)
        elif re.match(r'^[\w-]+@[0-9a-zA-Z-]{1,13}(\.\w{2,})+$',phone):

            self.logger.info("我是邮箱")
            cur =self.cur[1]
            #,TARGET_NO as '转赠号码'
            sql = "select BIND_NO as '领券号码' , PCARD_PASSWD as '卡密' ,LIFECYCLE_ST as '状态' ,BIND_TIME as '领取时间',USE_TIME as '使用时间'',USE_ORDER_ID as '充值订单号',BATCH_ID as '批次id'  from echnmarketdb.td_pcard_info_u where bind_no ='%s'" % (phone)
            #logger.error(sql)
        else:
            self.logger.error("号码格式不对："+phone+' 长度：'+str(len(phone)))
            self.falseCount += 1
            self.failedUser.append(phone)
            return
        #self.logger.error(sql)
        try:

            qryHead,qryResult = dbOper.qryData(cur, sql)

            if qryResult==[] or qryResult==None :
                self.falseCount += 1
                self.failedUser.append(phone)
                self.logger.error("No result for user:"+phone)
            else:
                self.trueCount += 1
                resultbody = []
                resulthead =[]
                for j in range(0,len(qryHead)):
                    resulthead.append(str(qryHead[j]))

                for i in range(0,qryResult.__len__()):


                    #用户卡券数据拼接
                    resultbody.append([]) #元素列表+1
                    for ii in range(0,len(qryResult[i])):
                        resultbody[i].append("'"+str(qryResult[i][ii]))



                #输出结果
                historyRuslt = open(historyResultFile, "a")#记录查询历史的文件history，也可以用来存输出结果。
                result.append('|'.join(resulthead))#首先写入头信息
                if xiaodongxi ==0 :
                    print(sql)
                    time.sleep(0.001)
                    print( '|'.join(resulthead))
                    historyRuslt.write(','.join(resulthead)+"\n")



                for i in range(0,len(resultbody)):
                    print('|'.join(resultbody[i]))#打印查询结果到控制台，logger.error 会打印时间戳，看起来乱
                    result.append('|'.join(resultbody[i]))
                    historyRuslt.write(','.join(resultbody[i])+"\n")

                historyRuslt.close()




        except Exception as e:
            logger.exception("Exception Logged run")
            self.falseCount += 1
            self.failedUser.append(phone)
            self.logger.error(e)
            self.logger.error ("Phone:%s Failed to Query -------------XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"%phone)
        return result #这个返回，，以后可能有用。

    def batchQuery(self,phoneFile):
        self.logger.error("Query start")
        with open(phoneFile,'r') as phoneList:
            xiaodongxi =0
            for info in phoneList:
                kami=info.split("|")[3]
                phone=info.split("|")[4]
                #print(phone,kami)
                result=self.query(phone,kami,xiaodongxi)
                xiaodongxi +=1


        time.sleep(0.05)
        self.logger.error("Query finished")
        self.logger.error("查询次数："+xiaodongxi.__str__()+"  查询结果计数："+self.trueCount.__str__())
        self.logger.error("\n错误计数："+self.falseCount.__str__()+"\n错误号码："+self.failedUser.__str__())

        return result




if __name__== "__main__":

    run = kaQ()

    phoneFile ='kamis'
    run.batchQuery(phoneFile)


