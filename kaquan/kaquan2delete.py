#!/tongweb/bin/python/python2.7/bin/python
#coding:utf8
import os
import sys
import time
import re
import sys
reload(sys)

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
        cur1,conn1 = dbOper.conn_db_new(config.local1_new)
        logger.info("连接数据库1成功")
    except Exception as e:
        print e
        logger.error("连接数据库1失败")
        cur1 = None
    try:
        cur2, conn2 = dbOper.conn_db_new(config.local2_new)
        logger.info("连接数据库2成功")
    except Exception as e:
        print e
        logger.error("连接数据库2失败")
        cur2 = None
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


    def query(self,phone,xiaodongxi):
        phone =phone.strip()
        result =[]
        #if len(phone) == 11 and  "@" not in str(phone):#注册用户需完善判断标准||手机号要正则
        if re.match(r'1\d{10}$',phone):
            self.logger.info ("我是手机号")
            module = int(phone[6:7])
            databaseNum = module%5 + 1
            tableNum =(phone[-1])

            if  module <= 4 :
                cur =self.cur[0]
                conn =self.conn[0]
            else:
                cur =self.cur[1]
                conn = self.conn[1]


            sql = "select serial_NUMber, EXPIRE_TIME,BIND_NO as '领券号码' ,BIND_TIME as '领取时间', PCARD_PASSWD as '卡密' ,LIFECYCLE_ST as '状态' ,LOCK_TIME AS  '锁定时间',USE_TIME as '使用时间',USE_ORDER_ID as '充值订单号',TARGET_NO as '转赠号码',BATCH_ID as '批次id' from echnmarketdb%d.td_pcard_info_%s where bind_no ='%s' and batch_id='1012635014808080384' " % (databaseNum, tableNum, phone)
            logger.info(sql)
            # sql = "select BIND_NO, PCARD_PASSWD ,LIFECYCLE_ST ,BIND_TIME,USE_TIME,USE_DATE,TARGET_NO from echnmarketdb%d.td_pcard_info_%s where bind_no ='%s'" % (databaseNum, tableNum, phone)
        elif re.match(r'^[\w-]+@[0-9a-zA-Z-]{1,13}(\.\w{2,})+$',phone):

            self.logger.info("我是邮箱")
            cur =self.cur[1]
            sql = "select BIND_NO as '领券号码' , PCARD_PASSWD as '卡密' ,LIFECYCLE_ST as '状态' ,BIND_TIME as '领取时间',USE_TIME as '使用时间'',USE_ORDER_ID as '充值订单号',TARGET_NO as '转赠号码',BATCH_ID as '批次id'  from echnmarketdb.td_pcard_info_u where bind_no ='%s'" % (phone)
            #logger.info(sql)
        else:
            self.logger.error("号码格式不对："+phone+' 长度：'+str(len(phone)))
            self.falseCount += 1
            self.failedUser.append(phone)
            return
        #self.logger.error(sql)
        try:

            print cur
            print(sql)
            cur.execute(sql)
            #conn.commit()


        except Exception as e:
            logger.exception("Exception Logged run")
            self.falseCount += 1
            self.failedUser.append(phone)
            self.logger.error(e)
            self.logger.error ("Phone:%s Failed to Query -------------XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"%phone)
        return

    def batchQuery(self,phoneFile):
        self.logger.error("Query start")
        with open(phoneFile,'r') as phoneList:
            xiaodongxi =0
            for phone in phoneList:
                result=self.query(phone,xiaodongxi)
                xiaodongxi =1


        time.sleep(0.05)
        self.logger.error("Query finished")
        self.logger.error("\n错误计数："+self.falseCount.__str__()+"\n错误号码："+self.failedUser.__str__())

        return result




if __name__== "__main__":

    run = kaQ()

    logger.error("脚本无参数执行，读取号码文件查询")
    phoneFile ='phoneNo'
    run.batchQuery(phoneFile)


