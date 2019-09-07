#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8

import os
import sys
import datetime
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper,createPath
from conf import config
import feeCheck

loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"fee","%s/ecora/log/"%BASE_DIR)



try:
    startDate = sys.argv[1]
    endDate = sys.argv[2]
    #month = startDate[:6]
except Exception as e:
    logger.error("没有指定日期，默认查询T-2账期数据")
    startDate =None
    endDate = None

def initDate():

    if startDate and endDate:
        month = startDate[:6]
        return startDate,endDate,month
    else:
        start_date = datetime.date.today() - datetime.timedelta(days=2)
        end_date = datetime.date.today() - datetime.timedelta(days=2)
        month = start_date.strftime('%Y%m')
        return  start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'),month

#class fee(object):# -_-!

def getConnect():
    try:
        cur,conn = dbOper.conn_db(config.ecora)
        logger.info("连接数据库成功")
    except Exception as e:
        logger.info(e)
        logger.error("连接数据库失败")
        cur,conn = None,None
    return  cur,conn

def gwSql(startDate, endDate, month):
    #startDate, endDate, month = initDate()

    gw_sql_all={}
    for trade_type in "C","R":
        if trade_type =="C":
            order_type = "0"
        else:
            order_type = "1"
        gw_sql_all[order_type]={}
        for pay_channel in "UNIPAY","WXPAY", "ALIPAY", "CMPAY":
            if pay_channel =="WXPAY":
                gw_sql_all[order_type][pay_channel]=("""select trade_date as '账期',count(1) as '个数',sum(fee) AS '{0}_{4}_fee'
                from td_ptl_gwpaytrade_msg{1} where pay_channel = '{0}' AND trade_type = '{4}' 
                and trade_date >= '{2}' and trade_date <= '{3}' group by trade_date order by trade_date""".format(pay_channel,month, startDate, endDate,trade_type))

            else:
                gw_sql_all[order_type][pay_channel] = {}
                if pay_channel =='UNIPAY':
                    for pay_method in ("M", "B","S"):
                        gw_sql_all[order_type][pay_channel][pay_method]=("""select trade_date as '账期',count(1) as '个数',sum(fee) AS '{0}_{1}_{5}_fee'
                        from td_ptl_gwpaytrade_msg{2} where pay_channel = '{0}' AND trade_type = '{5}' and pay_method = '{1}' 
                        and trade_date >= '{3}' and trade_date <= '{4}' group by trade_date order by trade_date""".format(pay_channel,pay_method,month, startDate, endDate,trade_type))
                else:
                    for pay_method in ("M", "B"):
                        gw_sql_all[order_type][pay_channel][pay_method] = ("""select trade_date as '账期',count(1) as '个数',sum(fee) AS '{0}_{1}_{5}_fee'
                        from td_ptl_gwpaytrade_msg{2} where pay_channel = '{0}' AND trade_type = '{5}' and pay_method = '{1}' 
                        and trade_date >= '{3}' and trade_date <= '{4}' group by trade_date order by trade_date""".format(
                        pay_channel, pay_method, month, startDate, endDate, trade_type))

    return gw_sql_all

def shopSql(startDate, endDate, month):
    #startDate, endDate, month = initDate()
    shop_sql_all={}
    for order_type in "0","1":
        if order_type =="0":
            order_status ="'2','4','5'"
        else:
            order_status = "'6','8','10','12','14'"
        shop_sql_all[order_type] = {}
        for pay_channel in "UNIPAY","WXPAY", "ALIPAY", "CMPAY":
            if pay_channel =="WXPAY":
                shop_sql_all[order_type][pay_channel]=(""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费{4}' from tw_ptl_payprocess_log{1}  
                where pay_date >= '{2}' and pay_date <= '{3}' and pay_channel = '{0}' AND ORDER_TYPE = '{4}' AND ORDER_STATUS IN ({5})  
                AND PAY_ORDER_TYPE = '0' 
                group by pay_date order by pay_date""".format( pay_channel,  month, startDate, endDate,order_type,order_status))

            else:
                shop_sql_all[order_type][pay_channel]={}
                if pay_channel =='UNIPAY':
                    for pay_method in ("M", "B","S"):
                        shop_sql_all[order_type][pay_channel][pay_method] =""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费{1}{5}' from tw_ptl_payprocess_log{2}  
                        where pay_date >= '{3}' and pay_date <= '{4}' and pay_channel = '{0}' AND ORDER_TYPE = '{5}' AND ORDER_STATUS IN ({6})  
                        AND PAY_ORDER_TYPE = '0' and pay_method='{1}' 
                        group by pay_date order by pay_date""".format( pay_channel, pay_method, month, startDate, endDate,order_type,order_status)
                else:
                    for pay_method in ("M", "B"):
                        shop_sql_all[order_type][pay_channel][pay_method] =""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费{1}{5}' from tw_ptl_payprocess_log{2}  
                        where pay_date >= '{3}' and pay_date <= '{4}' and pay_channel = '{0}' AND ORDER_TYPE = '{5}' AND ORDER_STATUS IN ({6})  
                        AND PAY_ORDER_TYPE = '0' and pay_method='{1}' 
                        group by pay_date order by pay_date""".format( pay_channel, pay_method, month, startDate, endDate,order_type,order_status)
    return shop_sql_all

def gen_file_name(shop_Or_Gw,*tags):
    tail = "_".join(list(tags))
    file_name = '%s/ecora/result/%s/%s.csv' % (BASE_DIR,shop_Or_Gw,tail)
    print file_name
    return  file_name

def query(sql,resultfile_name):
    resultfile =open(resultfile_name,"a")
    try:
        cur, conn = getConnect()
        qryHead,qryResult = dbOper.qryData(cur, sql)

        resultbody = []
        #resulthead =[]

        # for j in range(0,len(qryHead)):
        #     resulthead.append(str(qryHead[j]))
        # if not os.path.getsize(resultfile_name):
        #     logger.error("--写入数据头")
        #     resultfile.write('|'.join(resulthead) + "\n")

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


def main(startDate, endDate, month):

    shop = shopSql(startDate, endDate, month)
    gw = gwSql(startDate, endDate, month)
    for data in shop,gw:
        if id(data) ==id(gw):
            shop_Or_Gw = 'gw'
        else:
            shop_Or_Gw = 'shop'
        logger.info("%s数据查询开始--"%shop_Or_Gw)
        for type , sqlss in data.items():
            print type
            for channel,sqls in sqlss.items():
                print channel
                if channel == "WXPAY": # WXPAY doesn't differentiate  pay_method
                    logger.info(sqls)
                    query(sqls,gen_file_name(shop_Or_Gw,month,channel,type))
                # elif channel == 'ALIPAY': # ALIPAY has no pay_method "B"
                #     query(sqls['M'],gen_file_name(shop_Or_Gw,month,channel,type))
                #     logger.info(sqls['M'])
                else:
                    for method,sql in sqls.items():
                        logger.info(sql)
                        query(sql, gen_file_name(shop_Or_Gw,month, channel,method, type))
        logger.info("%s数据查询结束--" % shop_Or_Gw)



if __name__=='__main__':

    main(*initDate())
    feeCheck.check( month=initDate()[2])