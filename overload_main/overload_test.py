#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8


import sys
import multiprocessing
import os
import time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog,createPath
from conf import config
import overloadSendcard



dataBase = config.ecora
# 当前时间20180126-183918
loggerDate = getDate.getToday()

logger = printLog.getLogger(loggerDate, "overload", createPath.mkdir(config.log_dir))

class runMe(object):
    def __init__(self,info,threadNum=1,startTime='08:30:00',endTime='20:30:00'):

        self.startSendTime = startTime
        self.threadNum =threadNum
        self.endSendTime = endTime
        self.coupon_info =info['coupon_info']
        self.phone_dir = info['phone_dir']
        self.result_dir = info['result_dir']
        self.phoneFileName =info['phone_file_name']




    def getPhoneFile(self):
        phone_file =self.phone_dir +str(os.sep)+self.phoneFileName
        if os.path.exists(r'%s'%(phone_file)):
            logger.info( "号码文件:%s" % phone_file)
        else:
            logger.error("号码文件不存在:"+phone_file)
        return  phone_file

    def getResultFile(self):
        result_file_name =self.phoneFileName.split('.')[0]+".done"
        result_file =self.result_dir+str(os.sep) +result_file_name
        if os.path.exists(r'%s'%(result_file)):
            logger.error( "结果文件%s 已存在, 追加写入" % result_file)

        else:
            logger.info("第一次生成结果文件:"+result_file)
        return  result_file
    #发送优惠券
    def run(self,*args):
        #time.sleep(10)
        print args

        #生成号码文件，并返回文件名
        phoneFile= self.getPhoneFile()
        resultFile = self.getResultFile()
        logger.info("发券开始" )
        if os.path.exists(phoneFile):
            if os.path.getsize(phoneFile):
                try:
                    sendCard = overloadSendcard.NewSendcard(logger, phoneFile,resultFile, self.coupon_info, self.threadNum, self.startSendTime, self.endSendTime)
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
    sep = str(os.sep)
    data_dir = BASE_DIR +sep +'Data'+sep+'overload'
    phone_dir =data_dir +sep+'waiting'
    result_dir =createPath.mkdir('%s%sfinished'%(data_dir,sep))
    #aa =os.popen('ls %s |grep .dat'%phone_dir)

    #for i in aa.readlines():
    processlist =[]
    ps =multiprocessing.Pool(1)

    for i in ["a.dat","b.dat"]:
        phone_file_name =i.strip()
        infos={
        'phone_dir' : phone_dir,
        'result_dir' : result_dir,
        'phone_file_name' : phone_file_name,
        #默认60渠道，所有券通用
        'coupon_info':{
        'channelId' : '60',
        'batchID' : ('1064805472026099712',),
        'prov_code' : '99'}
        }
        thread_num =1 #可选参数，默认1
        startSendTime='08:00:00'#可选参数，默认08：30：00
        endSendTime='20:30:00'#可选参数，默认20：30：00
        rr = runMe(infos,threadNum=thread_num,startTime=startSendTime,endTime=endSendTime)
        #processlist.append(multiprocessing.Process(target=rr.run,args=()))
        ps.apply_async(func=run_me,args=(rr,i))#进程池中只能放函数，不能放实例的方法
    ps.close()
    ps.join()


