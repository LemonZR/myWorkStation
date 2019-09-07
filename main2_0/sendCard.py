#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8

import os
import sys
import time
import json
import requests
import random
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import responseCheck

'''
#获取上级目录
print os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
print os.path.abspath(os.path.dirname(os.getcwd()))
print os.path.abspath(os.path.join(os.getcwd(), ".."))
'''


def getMMtime():
    mm = int(time.time() * 1000).__str__()[-3:]
    t = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    return t + mm


def send(phone, phoneFileName, prov_code, channelId, batchID):
    # 创建请求生产
    random_number = random.randint(100000, 999999).__str__()
    serialNumber = channelId + prov_code +random_number+getMMtime()
    obtainDate = time.strftime('%Y%m%d', time.localtime(time.time()))
    req = requests.session()
    url = "http://10.255.254.94:8012/coupons/v2/pcardrest/pcardReceive"
    header = {'content-type': 'application/json',
              'cookie': 'promotion_token=3dwrf34fweq3rfq34qww4y66'}
    params = """{"loginNo":"%s", "loginType":"0", "channelId":"%s", "oprType":"2", "batchID":"%s",  "serialNumber":"%s", "obtainDate":"%s"}""" % ( phone, channelId, batchID, serialNumber, obtainDate)

    retCode =None
    request_num =-1
    mark = []
    retCodeList = [
        '000000','060201','060202','060203','060204','060205','060206','060207','060208',
        '060209','060210','060211', '060212','060213','060214','060215','060216','060217'
        ,'060218','000300','100001','100003','100004','100006', '100007','500003']

    while retCode not in(retCodeList) and request_num <5:
        request_num += 1
        response = req.request(method='POST', url=url, data=params, headers=header, timeout=1000 , proxies=None)
        try:

            if response.status_code == 200:
                try:
                    result = json.loads(response.text)
                except Exception as e:
                    if type(response) is str:
                        result = json.loads(response)
                    else:
                        result = json.loads(response.content)
                retCode = result["retCode"]
                if retCode =="000000":
                    state = True
                else:
                    state = False
                result = {"retCode": retCode, "info": result["retMsg"] , "state": state}

            else:
                result = {"retCode": "800002", "info": u"status code error %s" % response.status_code, "state": False}

        except Exception as e:

            if response == "ConnectTimeout":
                result = {"retCode": "800003", "info": u"与服务器连接超时【ConnectTimeout】", "state": False}
            elif response == "Timeout":
                result = {"retCode": "800004", "info": u"响应超时【Timeout】", "state": False}
            elif response == "ConnectionError":
                result = {"retCode": "800005", "info": u"网络受限【ConnectionError】", "state": False}
            else:
                result = {"retCode": "800006", "info": u"系统异常【Response】", "state": False}

        mark.append("%s"%result["retCode"])
    retCode, retMsg, status = "-".join(mark), result["info"],result["state"]

    print (response.content,request_num)

    b = phone + "," + batchID + "," + str(retCode) + "," + retMsg.encode("utf-8") + "," + str(status) + "," + serialNumber +","+ "repeat-%s"%request_num# 保存结果文件

    result_write(phoneFileName, b)




def result_write(file_name, b):
    result_file = open('%s.txt' % (file_name), "a")
    result_file.write('%s' % (b))
    result_file.write("\n")


def run(phone_file, prov_code, channelId, batchID):
    with open('%s.dat' % (phone_file), 'r') as csvfile:
        for lines in csvfile:
            phone = lines.strip()
            # 发送http触点信息重发请求
            send(phone, phone_file, prov_code, channelId, batchID)


if __name__ == '__main__':
    send("15032609451", "./vxxxxxx", "891", "60", "1045871361450442752")