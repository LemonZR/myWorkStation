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
                shop_sql_all[order_type][pay_channel]=(""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费0' from tw_ptl_payprocess_log{1}  
                where pay_date >= '{2}' and pay_date <= '{3}' and pay_channel = '{0}' AND ORDER_TYPE = '{4}' AND ORDER_STATUS IN ({5})  
                AND PAY_ORDER_TYPE = '0' 
                group by pay_date order by pay_date""".format( pay_channel,  month, startDate, endDate,order_type,order_status))

            else:
                shop_sql_all[order_type][pay_channel]={}
                if pay_channel =='UNIPAY':
                    for pay_method in ("M", "B","S"):
                        shop_sql_all[order_type][pay_channel][pay_method] =""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费{1}0' from tw_ptl_payprocess_log{2}  
                        where pay_date >= '{3}' and pay_date <= '{4}' and pay_channel = '{0}' AND ORDER_TYPE = '{5}' AND ORDER_STATUS IN ({6})  
                        AND PAY_ORDER_TYPE = '0' and pay_method='{1}' 
                        group by pay_date order by pay_date""".format( pay_channel, pay_method, month, startDate, endDate,order_type,order_status)
                else:
                    for pay_method in ("M", "B"):
                        shop_sql_all[order_type][pay_channel][pay_method] =""" select pay_date,count(1) as "count",sum(convert(bigint, fee))as '{0}服务费{1}0' from tw_ptl_payprocess_log{2}  
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


def run_qry(startDate, endDate, month,shop_Or_Gw='shop',channel='CMPAY',method='M',order_type="1"):
    shop = shopSql(startDate, endDate, month)
    gw = gwSql(startDate, endDate, month)

    if shop_Or_Gw =='shop':
        sql_dict =shop
    elif shop_Or_Gw =='gw':
        sql_dict = gw
    logger.info("%s数据查询开始--"%shop_Or_Gw)
    if channel == "WXPAY": # WXPAY doesn't differentiate  pay_method
        sql =sql_dict[order_type][channel]
        logger.info(sql)
        query(sql,gen_file_name(shop_Or_Gw,month,channel,order_type))

    else:
        sql =sql_dict[order_type][channel][method]
        logger.info(sql)
        query(sql, gen_file_name(shop_Or_Gw,month, channel,method, order_type))
    logger.info("%s数据查询结束--" % shop_Or_Gw)



if __name__=='__main__':
    try:

        shop_Or_Gw =sys.argv[1]# shop  /  gw
        channel = sys.argv[2] # WXPAY,CMPAY.....
        method = sys.argv[3] # M / B
        order_type =str(sys.argv[4]) # 0 / 1
        star_date =str(sys.argv[5])
        end_date =str(sys.argv[6])
        month =str(star_date[:6])
    except Exception as e:
        sys.exit("""请指定以下参数：
        shop_or_gw =sys.argv[1]   #shop  /  gw
        pay_channel = sys.argv[2] #WXPAY,CMPAY.....
        pay_method = sys.argv[3]  #M / B / S
        order_type =sys.argv[4]   #0 / 1
        star_date =sys.argv[5]    #20190102
        end_date =sys.argv[6]     #20190102
        """)

    run_qry(star_date,end_date,month,shop_Or_Gw=shop_Or_Gw,channel=channel,method=method,order_type=order_type)