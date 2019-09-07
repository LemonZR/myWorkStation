#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8

import os
import sys
import datetime
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper,createPath
import findDiffOrderId

loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"feeCheck","%s/ecora/log/"%BASE_DIR)
two_days_ago = datetime.date.today() - datetime.timedelta(days=2)
check_date = two_days_ago.strftime('%Y%m%d')
def get_lastest_data(result_file_name):
    data = {}
    with open(result_file_name, "r") as r:
        for line in r.readlines():
            line_list = line.strip().split("|")
            date, count, fee = line_list
            data[date] = [count, fee]
    return data

def pack_data( month):
    dict ={}
    for shop_or_gw in "shop","gw":
        dict[shop_or_gw]={}
        for order_type in "0","1":
            for pay_channel in "UNIPAY","WXPAY", "ALIPAY", "CMPAY":
                if pay_channel =="WXPAY":
                    file_name = '%s/ecora/result/%s/%s_%s_%s.csv' % (BASE_DIR,shop_or_gw, month,pay_channel,order_type)

                    index =pay_channel+"_"+order_type
                    dict[shop_or_gw][index]=get_lastest_data(file_name)
                else:
                    if pay_channel =="UNIPAY":
                        for pay_method in ("M", "B","S"):
                            file_name = '%s/ecora/result/%s/%s_%s_%s_%s.csv' % (BASE_DIR,  shop_or_gw,month, pay_channel,pay_method,order_type)
                            index =  "_".join([pay_channel,pay_method,order_type])
                            dict[shop_or_gw][index] = get_lastest_data(file_name)
                    else:
                        for pay_method in ("M", "B"):
                            file_name = '%s/ecora/result/%s/%s_%s_%s_%s.csv' % (BASE_DIR,  shop_or_gw,month, pay_channel,pay_method,order_type)
                            index =  "_".join([pay_channel,pay_method,order_type])
                            dict[shop_or_gw][index] = get_lastest_data(file_name)
    return dict

def get_diff_order_id(channel_mix,date,result_file):
    date = ''.join(date.split("-"))

    tmp =channel_mix.split("_")
    if len(tmp) ==3:
        channel, pay_method, order_type =tmp
    elif len(tmp) ==2:
        channel,order_type =tmp
        pay_method ='empty'
    else:
        logger.error("channel_mix is wrong !")
    findDiffOrderId.query_diff_order_id(date,channel,pay_method,order_type,result_file )


def check(month=None,isCrontab='yes',checkdate=check_date):

    dict = pack_data(month)
    sum ={}

    #备份历史result文件
    # \ecora\result\checkresult
    check_result_dir = str(os.sep).join([BASE_DIR,"ecora","result","checkresult"])
    # \ecora\result\checkresult_his
    check_result_bak_dir = str(os.sep).join([BASE_DIR,"ecora","result","checkresult_his"])
    union_result_file = check_result_dir + str(os.sep)+month +".xls"
    diff_file = check_result_dir + str(os.sep)+month +".diff"
    miss_file = check_result_dir + str(os.sep)+month +".miss"
    comd ='mv ' +check_result_dir+str(os.sep)+'*.*  '+ check_result_bak_dir
    print comd
    os.system(comd)

    for channel,date_data in dict["gw"].items():
        sum[channel] ={"count":0,"fee":0}
        check_result_file = '%s/%s_%s.csv' % (check_result_dir, month, channel)
        w = open(check_result_file, 'a')
        for date,data in date_data.items():
            try:
                shop_data =  dict["shop"][channel][date]
                if data != shop_data:
                    print("\n%s账期%s的数据不一致!\n网关:%s 商城:%s\n"%(channel,date,data,shop_data))
                    dif_f = open(diff_file, "a")
                    dif_f.write("%s账期%s网关:%s 商城:%s\n"%(channel,date,data,shop_data))
                    if ( isCrontab == 'yes' and date == checkdate ) or isCrontab == 'no':#定时任务，且账期为T-2(check_date)才查询差异:
                        try:
                            print "开始查询差异订单号========="
                            get_diff_order_id(channel,date,diff_file)
                            print "查询结束，差异见%s"%diff_file
                        except Exception as e:
                            logger.error("查询差异订单失败")
                    else:
                        logger.error("仍存在非T-2(%s)账期差异，请人工核查 "%check_date)

                else:
                    print("[%s %s]网关: %s 商城: %s "%(channel,date,data,shop_data))
                sum[channel]["count"] += int(shop_data[0])
                sum[channel]["fee"] += int(shop_data[1])
                w.write("%s|%s|%s\n"%(date,shop_data[0],shop_data[1]))
            except Exception as e:
                miss_f = open(miss_file, "a")
                miss_f.write("[ %s %s ]商城该账期无数据或其他原因\n"%(date,channel))
                logger.error("[ %s %s ]商城该账期无数据或其他原因\n"%(date,channel))

    with open(union_result_file,"a") as f:
        for channels,datas in sum.items():
            line = channels+"|"+str(datas["count"])+"|"+str(datas["fee"])
            f.write(line+"\n")





if __name__=='__main__':

    try:
        month =sys.argv[1]

    except Exception as e:
        month = two_days_ago.strftime('%Y%m')
        print "稽核月份 %s" %month
    check(month,isCrontab='yes')
