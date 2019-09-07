#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8


import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper
from conf import config
loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"find_diff","%s/ecora/log/"%BASE_DIR)
def __getConnect():
    try:
        cur,conn = dbOper.conn_db(config.ecora)
        logger.info("连接数据库成功")
    except Exception as e:
        logger.info(e)
        logger.error("连接数据库失败")
        cur,conn = None,None
    return  cur,conn

def __diff_sql(date,pay_channel,pay_method,order_tye):

    if order_tye == '0':
        trade_type ='C'
    else:
        trade_type ='R'
    month =date[:6]
    if pay_channel =='WXPAY' and pay_method =='empty':
        sql ="""select ORDER_ID,pay_method,fee,pay_date from  tw_ptl_payprocess_log{0} where ORDER_TYPE ='{5}' and PAY_ORDER_TYPE ='0' and (pay_channel != '{4}' or fee <0) and pay_date = '{1}'  and order_id  in
          ( select ORDER_ID
          from td_ptl_gwpaytrade_msg{0} where  trade_type = '{2}' and  pay_channel = '{4}' 
          and trade_date = '{1}' )
        """ .format(month, date, trade_type,pay_method,pay_channel,order_tye )
    else:
        sql= """
        select ORDER_ID,pay_method,fee,pay_date from  tw_ptl_payprocess_log{0} where  (pay_method !='{3}'  or fee <0)
        and ORDER_TYPE ='{5}' and PAY_ORDER_TYPE ='0' and pay_channel = '{4}' and pay_date = '{1}'  and order_id  in
          ( select ORDER_ID
          from td_ptl_gwpaytrade_msg{0} where  trade_type = '{2}' and  pay_channel = '{4}' AND pay_method = '{3}'
          and trade_date = '{1}' )
        """ .format(month, date, trade_type,pay_method,pay_channel,order_tye )

    return sql

def __query(sql,resultfile_name):
    resultfile =open(resultfile_name,"a")
    try:
        cur, conn = __getConnect()
        logger.info("查询开始: %s" % sql)
        qryHead,qryResult = dbOper.qryData(cur, sql)
        logger.info("查询结束: %s" % sql)
        resultbody = []

        if qryResult.__len__():
            for i in range(0,qryResult.__len__()):
                #查询结果
                resultbody.append([]) #长度+1
                for ii in range(0,len(qryResult[i])):
                    resultbody[i].append(str(qryResult[i][ii]))
            for i in range(0,len(resultbody)):
                resultfile.write('|'.join(resultbody[i])+"\n")
        else:
            logger.error("查询结果为空:%s"%sql)
    except Exception as e:
        logger.error("%s \n--------执行失败"%sql)
        logger.error(e)
def query_diff_order_id(date,pay_channel,pay_method,order_tye,result_file):
    sql = __diff_sql(date,pay_channel,pay_method,order_tye)
    __query(sql, result_file)
if __name__=="__main__":
    cur, conn = __getConnect()
    sql =__diff_sql("20181102","CMPAY","M","1")
    result_file =os.sep.join([BASE_DIR ,"ecora" , "checkresult" , "diff_order_id.xls"])
    print sql
