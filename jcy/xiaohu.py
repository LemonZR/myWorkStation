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
logger=printLog.getLogger(loggerDate,"jcy","%s/jcy/log/"%BASE_DIR)

def getConnect():
    try:
        cur,conn = dbOper.conn_db_new(config.jcy)
        logger.info("连接数据库成功")
    except Exception as e:
        logger.error("连接数据库失败")
        cur,conn = None,None
    return  cur,conn
class jcy(object):
    def __init__(self):
        #self.count =0
        self.cur,self.conn =getConnect()
        self.logger = logger
        if self.cur ==None:
            logger.error("初始化数据库连接失败，退出")
            sys.exit(1)


    def query(self,phones ):

        for phone in phones:

            sql = """update td_padm_login_info set valid_flag = 1 where CONTACT_PHONE = %s """ %phone
            logger.info(sql)

            try:
                dbOper.qryData(self.cur, sql)
                self.conn.commit()
            except Exception as e:
                self.logger.error(e)
                self.logger.error ("销户失败：%s" %e.__str__())
            sql = """select valid_flag,CONTACT_PHONE from  td_padm_login_info  where CONTACT_PHONE = %s """ %phone
            logger.info(sql)
            qryHead, qryResult = dbOper.qryData(self.cur, sql)
            logger.info(list(qryResult))
            print qryResult



    def batchQuery(self,tablesinfo):
        self.logger.error("Query start")
        with open(tablesinfo,'r') as login_no_list:
            for login_no in login_no_list:
                self.query(login_no.strip())



if __name__== "__main__":

    run = jcy()
    try:
        phones = str(sys.argv[1]).strip()
        print "phone is: "+ phones
        phones =(phones,)
        run.query(phones)

    except Exception as e:
        logger.error("多个号码请用英文逗号隔开")
        roles ='roles.conf'