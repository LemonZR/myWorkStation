#!/echnlog/tools/bin/python
#coding:utf8


import os
import sys
import  time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog,createPath
from conf import config
import mutiSendcard



dataBase = config.ecora
# 当前时间20180126-183918
loggerDate = getDate.getToday()


# yesterday='20180125'
class runMe(object):
    def __init__(self,coupon_info,threadNum=1,startTime='08:30:00',endTime='20:30:00'):
        self.actName = coupon_info['actName']
        self.startSendTime = startTime
        self.threadNum =threadNum
        self.endSendTime = endTime
        self.coupon_info =coupon_info
        self.resData = createPath.mkdir(config.data_dir+'/'+self.actName)
        self.logger =  printLog.getLogger(loggerDate,self.actName,createPath.mkdir(config.log_dir))
        self.phoneFileName = self.actName


    def getPhoneFile(self):
        phone_file ='%s/%s.dat' % (self.resData, self.phoneFileName)
        logger = self.logger
        if os.path.exists(r'%s'%(phone_file)):
            print '号码文件已经生成，请注意是否需要重新生成'
            print "号码文件:%s" % phone_file
            logger.error("号码文件已经生成，请注意是否需要重新生成")
        else:
            print "号码文件不存在:"+phone_file
        print phone_file
        return  '%s/%s' % (self.resData, self.phoneFileName)
    #发送优惠券
    def run(self):
        #生成号码文件，并返回文件名
        phoneFile= self.getPhoneFile()
        logger = self.logger
        logger.error("发券开始" )
        if os.path.exists(phoneFile+'.dat'):
            if os.path.getsize(phoneFile+'.dat'):
                try:

                    sendCard = mutiSendcard.NewSendcard(phoneFile, self.coupon_info,self.threadNum,self.startSendTime,self.endSendTime)
                    sendCard.mutiRun()
                    logger.info("发券完成 success")
                    print ("发券完成")
                except Exception as e:

                    print e
                    logger.info(e)
                    logger.error("发券失败 failed")
            else:
                print '号码文件为空,本次程序终止！'
                return
        else:
            print  '号码文件不存在,本次程序终止！'

if __name__ == "__main__":

    coupon_info={
    'actName' :'normal',
    #默认60渠道，所有券通用
    'channelId' : '60',
    'batchID' : ('1012623968194138112',),
    'prov_code' : '471'
    }
    thread_num =1 #可选参数，默认1
    startSendTime='08:30:00'#可选参数，默认08：30：00
    endSendTime='20:30:00'#可选参数，默认20：30：00
    rr = runMe(coupon_info,threadNum=thread_num,startTime=startSendTime,endTime=endSendTime )
    try :
        cmd = sys.argv[1].strip()
        if cmd == 'run':
            rr.run()
        else :
            exit("参数不对，或不要输入参数")
    except Exception :
        commond = raw_input("该批次id为：%s 是否要进行卡券下发:[yes],[no]? "%coupon_info['batchID'])
        if commond.strip() =='yes':
            rr.run()
        else :
            exit("退出咯")