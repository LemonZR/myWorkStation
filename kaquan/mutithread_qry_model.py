#!/echnlog/tools/bin/python
#coding:utf8


import os
import time
import sys,threading
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
loggerDate = getDate.getTime()
logger=printLog.getLogger(loggerDate,"kaquan","%s/kaquan/log/"%BASE_DIR)

def getConnect():
    try:
        return config.kqzx2_0_1,config.kqzx2_0_2


    except Exception as e:
        return  None,None


class kaQ(object):
    def __init__(self):
        self.count =0
        self.connect_info =getConnect()
        self.logger = logger
        if self.connect_info ==[None,None]:
            logger.error("初始化数据库连接失败，退出")
            #sys.exit(1)

    def get_sql(self,sql_format ):
        select_from , where_condition =sql_format.split("table_name")
        sqls =[]
        for databaseNum in range(1,6):#range(1,6)---> 序号1到5的数据库
            for phoneTail in range(0,10):#号码尾号0-9
                sql=select_from + 'echnmarketdb%s.td_pcard_info_%s '% (databaseNum,phoneTail)+ where_condition
                sqls.append(sql)
                print sql
        return sqls

    def mutithread_query(self,sql_format,res_file_name):
        sqls =self.get_sql(sql_format)

        res_file = open(res_file_name,"a")
        thread_id=0
        for conn_data in self.connect_info:
            thread_list = []
            for sql in sqls:
                connectInfo = dbOper.conn_db(conn_data)
                thread_id += 1
                thread_list.append(threading.Thread(target=self.qry,args=(sql,res_file,connectInfo),name=thread_id))

            time.sleep(0.05)
            logger.info("线程装载完毕，开始执行")
            for thread in thread_list:
                thread.start()
                logger.info("%s 线程开始",thread.name)

            for thread in thread_list:
                thread.join()
            time.sleep(0.005)
            self.logger.error("Query finished")
            logger.error("计数：" + str(self.count))
            time.sleep(10)
            print "本组执行完毕，累计%s个sql"%thread_id


    def qry(self,sql,res_file,connectInfo):

        historyResult = res_file
        resultbody = []
        time.sleep(5)
        print connectInfo
        try:
            logger.info(sql)
            cur ,conn= connectInfo
            qryHead, qryResult = dbOper.qryData(cur, sql)

            if self.count == 0:
                resulthead = []
                for j in range(0, len(qryHead)):
                    resulthead.append(str(qryHead[j]))
                historyResult.write(','.join(resulthead) + "\n")
            self.count += len(qryResult)
            for i in range(0, qryResult.__len__()):
                # 查询结果
                resultbody.append([])  # 长度+1
                for ii in range(0, len(qryResult[i])):
                    resultbody[i].append(str(qryResult[i][ii]))
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(e)
        for i in range(0, len(resultbody)):
             historyResult.write('|sp'.join(resultbody[i]) + "\n")
        return resultbody

if __name__== "__main__":
    start_time =time.strftime('%Y%m%d %H:%M:%S',time.localtime( time.time()))
    run = kaQ()

    '''table_name 是 分隔符'''
    sql_format = '''select SQL_NO_CACHE *
from table_name  where BIND_TIME >= '2019/04/04 00:00:00' and bind_time <='2019/04/09 23:59:59' and batch_id = 1113664165458870272 '''
#select * from echnmarketdb%d.td_pcard_info_%s where BIND_TIME >= '2019/04/04 00:00:00' and bind_time <='2019/04/09 23:59:59' and batch_id=1113664165458870272
    result_file_name = str(os.sep).join([BASE_DIR, "kaquan", "result", "20190411"])

    run.mutithread_query(sql_format,result_file_name)
    print start_time,time.strftime('%Y%m%d %H:%M:%S',time.localtime( time.time()))
