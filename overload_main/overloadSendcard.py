# coding:utf8

import os
import sys
import time
import random
import shutil
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True

from Utils import printLog,getDate
from conf import config
import threading,Queue


'''
#获取上级目录
print os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
print os.path.abspath(os.path.dirname(os.getcwd()))
print os.path.abspath(os.path.join(os.getcwd(), ".."))
'''

class NewSendcard(object):
    try:
        def __init__(self,logger,phone_file,result_file,coupon_info,threadNum=1,startTime='08:30:00',endTime='20:30:00'):
            self.reqSes = requests.session()
            self.startSendTime = startTime
            self.endSendTime = endTime
            self.threadNum = threadNum
            self.reqSes.keep_alive=False
            self.reqSes.adapters.DEFAULT_RETRIES = 5
            self.phone_file = phone_file
            self.result_file = result_file
            self.prov_code = coupon_info['prov_code']
            self.channelId = coupon_info['channelId']
            self.batchIDs = coupon_info['batchID']
            self.phone_que =Queue.Queue()
            self.logger =logger
            self.failedLogger =printLog.getLogger(getDate.getTime(),"unsend",config.log_dir)
    except Exception as e :
        print(e)

    def getMMtime(self):
        mm = int(time.time() * 1000).__str__()[-3:]
        t = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        return t + mm

    def send(self,phone,batchID):

        # 创建请求生产
        reqSes = self.reqSes
        random_number = random.randint(100000, 999999).__str__()
        serialNumber = self.channelId + self.prov_code +random_number+self.getMMtime()
        obtainDate = time.strftime('%Y%m%d', time.localtime(time.time()))
        url = "http://10.255.254.94:8012/coupons/v2/pcardrest/pcardReceive"
        header = {'content-type': 'application/json','cookie': 'promotion_token=3dwrf34fweq3rfq34qww4y66'}

        params = """{"loginNo":"%s", "loginType":"0", "channelId":"%s", "oprType":"2", "batchID":"%s",  "serialNumber":"%s", "obtainDate":"%s"}""" % ( phone, self.channelId, batchID, serialNumber, obtainDate)
        retCode = None
        request_num = -1
        mark = []
        retCodeList = [
            '000000', '060201', '060202', '060203', '060204', '060205', '060206', '060207', '060208',
            '060209', '060210', '060211', '060212', '060213', '060214', '060215', '060216', '060217'
            , '060218', '000300', '100001', '100003', '100004', '100006', '100007', '500003']

        while retCode not in (retCodeList) and request_num < 5:
            request_num += 1
            #response = reqSes.request(method='POST', url=url, data=params, headers=header, timeout=15, proxies=None)
            response = ''
            try:
                #self.logger.info(response)
                #self.logger.info(response.content)
                if response.status_code == 200:
                    try:
                        result = response.json()
                        retCode = result["retCode"]
                        if retCode == "000000":
                            state = True
                        else:
                            state = False
                        result = {"retCode": retCode, "info": result["retMsg"], "state": state}
                    except Exception as e:
                        result = {"retCode": "800001", "info": u"响应报文解析异常", "state": False}

                else:
                    result = {"retCode": "800002_%s"% response.status_code, "info": u"status code error " ,"state": False}

            except Exception as e:
                if response == "ConnectTimeout":
                    result = {"retCode": "800003", "info": u"与服务器连接超时【ConnectTimeout】", "state": False}
                elif response == "Timeout":
                    result = {"retCode": "800004", "info": u"响应超时【Timeout】", "state": False}
                elif response == "ConnectionError":
                    result = {"retCode": "800005", "info": u"网络受限【ConnectionError】", "state": False}
                else:
                    result = {"retCode": "800006", "info": u"系统异常【Response】", "state": False}
            mark.append("%s" % result["retCode"])
        retCode, retMsg, status = "-".join(mark), result["info"], result["state"]


        b = phone + "," + batchID + "," + str(retCode) + "," + retMsg.encode("utf-8") + "," + str(status) + "," + serialNumber + "," + "repeat-%s" % request_num  # 保存结果文件
        return b



    def _init_phone_queue(self):
        with open('%s' % (self.phone_file), 'r') as csvfile:
            self.logger.info("self.phone_file"+self.phone_file)
            for phone in csvfile:
                self.phone_que.put(phone.strip())
            self.logger.error("号码队列就绪--------------------------"+self.phone_que.qsize().__str__())
    def move_file(self):
        done_dir = os.sep.join([BASE_DIR,"Data","overload","done",''])
        print done_dir
        try:
            shutil.move(self.phone_file,done_dir)
            self.logger.info("文件 %s移动到了%s"%(self.phone_file,done_dir))
        except Exception as e:
            print e
    def runSend(self,tName,reult_file):
        while True:
            now = time.strftime('%Y%m%d %H:%M:%S', time.localtime(time.time()))
            startTime =time.strftime('%Y%m%d '+self.startSendTime,time.localtime(time.time()))
            endTime = time.strftime('%Y%m%d '+self.endSendTime,time.localtime(time.time()))
            if  now >= startTime  and now <= endTime :
                try:
                    phone =self.phone_que.get(block=False)
                    try:
                        #如果有多张券发多张券
                        for batch_id in self.batchIDs:
                            result = self.send(phone,batch_id)
                            reult_file.write(result+'\n')
                    except Exception as e:
                        self.failedLogger.error(e.__str__()+":\n"+phone)
                except Exception as e:
                    self.logger.exception("队列为空")
                    break
            else:
                self.logger.error(tName + " : I am sleeping ...")
                time.sleep(600)
                self.logger.error(tName + " : I am up ...")




    def mutiRun(self):
        threads=[]
        result_file = open('%s'%self.result_file,'a')
        _init_phone_que = threading.Thread(target= self._init_phone_queue, name="_init_phone_que", args=())
        _init_phone_que.start()
        time.sleep(0.1)
        print "thread_num is : " +str(self.threadNum)
        for i in range(0,self.threadNum):
            tName = ('%06d' % i).__str__()

            t = threading.Thread(target=self.runSend, name=tName, args=(tName,result_file))
            threads.append(t)
        for thread in threads:
            thread.setDaemon(True)#设置为守护线程后，就要join一下，不然，子线程未执行完，主线程就结束了，子线程也退出了。
            print thread.getName()+  '开始发券'
            thread.start()

        _init_phone_que.join()
        #确保每个线程执行完
        for thread in threads:
            thread.join()
            self.logger.error("%s 执行完成了" %thread.getName())
        self.move_file()
        self.logger.error('result file is %s'%self.result_file)



if __name__ == '__main__':
    print "别摸我"
