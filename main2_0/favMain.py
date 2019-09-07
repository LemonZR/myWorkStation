#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8


import sys
import multiprocessing
import os
import time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog,createPath,dbOper
from conf import config
import favSendCard
import datetime


dataBase = config.ecora2
# 当前时间20180126-183918
loggerDate = getDate.getToday()
yesterday = getDate.getYesterday()
loggerTime =getDate.getMMtime()
logger = printLog.getLogger(loggerDate, "favCharge", createPath.mkdir(config.log_dir))
#5分钟前
now_time = datetime.datetime.now()
start_time = (now_time - datetime.timedelta(minutes=5)).strftime("%Y/%m/%d %H:%M:00")
end_time = now_time.strftime("%Y/%m/%d %H:%M:00")
table_tail =now_time.strftime("%Y%m%d")
class runMe(object):
    def __init__(self,info,threadNum=1,startTime='08:30:00',endTime='20:30:00'):

        self.startSendTime = startTime
        self.threadNum =threadNum
        self.endSendTime = endTime
        self.coupon_info =info['coupon_info']
        self.phone_dir = info['phone_dir']
        self.result_dir = info['result_dir']
        self.phoneFileName =info['phone_file_name']
    def getSql(self):
        """
        8分钟前
        :return:
        """
        # when charge_fee >= 20000 and charge_fee <30000 then '1082108089244712960'
        # when charge_fee >= 30000 then '1082108839832190976'

        sql = """select  phone_no,
        case 
        when charge_fee >= 10000  then '1082101642897260544'
        .....#省略
        else 'eception'
        end  as batch_id,
        charge_fee
        from tw_ptl_payprocess_log%s   where
          rate_operate_id ='2429' and prov_code='220'  
        and order_type='0'
        and  order_status in ('2','4','5')and charge_fee >= 10000 and gw_pay_time >='%s' and gw_pay_time <'%s'
        """%(table_tail,start_time,end_time)

        return sql

    def getPhoneFile(self,):
        phone_file =self.phone_dir +str(os.sep)+self.phoneFileName
        if os.path.exists(r'%s'%(phone_file)):
            print "号码文件:%s" % phone_file
            logger.info("号码文件已经生成，请注意是否需要重新生成,号码文件:%s" % phone_file)
        else:
            conn_data = dataBase
            logger.info("开始连接数据库")
            try:
                cur, conn = dbOper.conn_db(conn_data)  # 获取数据库连接cur
                logger.info("连接数据库成功")
            except Exception as e:
                logger.info("连接数据库失败")
            try:
                sql = self.getSql()
                logger.info("Query starting with sql : " + sql )
                head,result = dbOper.qryData(cur,sql)
                logger.info("Query end ")
                logger.info('开始生成号码文件')
                try:
                    with open(phone_file, "a") as phoneFile :
                        for res in result:
                            phoneFile.write(",".join(map(str, res)))
                            phoneFile.write("\n")
                except Exception as e:
                    print e
                print "号码文件生成结束:%s" % phone_file
                phoneFile.close()
            except Exception as e:
                print("查询数据失败")

        return  phone_file




    def getResultFile(self):
        result_file_name =self.phoneFileName.split('.')[0]+".done"
        result_file =self.result_dir+str(os.sep) +result_file_name
        if os.path.exists(r'%s'%(result_file)):
            logger.info( "结果文件%s 已存在, 追加写入" % result_file)

        else:
            logger.error("第一次生成结果文件:"+result_file)
        return  result_file
    #发送优惠券
    def run(self,*args):
        #time.sleep(10)
        #生成号码文件，并返回文件名
        phoneFile= self.getPhoneFile()
        resultFile = self.getResultFile()
        logger.info("发券开始" )
        if os.path.exists(phoneFile):
            if os.path.getsize(phoneFile):
                try:
                    sendCard = favSendCard.NewSendcard(logger, phoneFile, resultFile, self.coupon_info, self.threadNum, self.startSendTime, self.endSendTime)
                    print sendCard.phone_que
                    sendCard.mutiRun()
                    logger.info("发券完成 success")
                    print ("发券完成")
                except Exception as e:

                    print e
                    logger.info(e)
                    logger.error("发券失败 failed")
            else:
                logger.error('号码文件%s为空,本次程序终止！'%phoneFile)
                return
        else:
            print  '号码文件不存在,本次程序终止！'
            return
def run_me(instance,*args):
    instance.run(args)
if __name__ == "__main__":

    act_name ="Tianjin"

    sep = str(os.sep)
    data_dir = BASE_DIR +sep +'Data'+sep+ "favCharge"
    phone_dir =data_dir +sep+'dat'
    result_dir =createPath.mkdir('%s%sfinished'%(data_dir,sep))

    phone_file_name = "%s_%s.dat"%(act_name,loggerTime)
    infos={
    'phone_dir' : phone_dir,
    'result_dir' : result_dir,
    'phone_file_name' : phone_file_name,
    #默认60渠道，所有券通用
    'coupon_info':{
    'channelId' : '60',
    #'batchID' : ('find from phonefile',),
    'prov_code' : '220'}
    }
    thread_num =1 #可选参数，默认1
    startSendTime='00:00:00'#可选参数，默认08：30：00
    endSendTime='23:59:59'#可选参数，默认20：30：00
    rr = runMe(infos,threadNum=thread_num,startTime=startSendTime,endTime=endSendTime)
    rr.run()
    print table_tail,start_time,end_time