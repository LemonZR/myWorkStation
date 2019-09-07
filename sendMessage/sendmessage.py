#coding:utf8

import os
import sys
import time
import json
import requests,random
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True



'''
#获取上级目录
print os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
print os.path.abspath(os.path.dirname(os.getcwd()))
print os.path.abspath(os.path.join(os.getcwd(), ".."))
'''

def send(phone, message, result_file):



    #创建请求生产
    req = requests.session()
    req.encoding = 'utf-8'
    sysID =1101
    sendTime =time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    seq = str(sysID)+ sendTime +random.randint(1000,9999).__str__()

    url = "http://10.255.201.230:9005/mnprest/v1/smssend"
    header = {'content-type': 'application/json'
            }

    params = '''{
    "hold1":"0",
    "loginNo":"13910809614",
    "paramMap":{"msg":"%s"},
    "phoneNo":"%s",
    "sendTime":"%s",
    "seq":"%s",
    "sysID":"%s",
    "templateid":"100000062"}'''%(message,phone,sendTime,seq,sysID)
    param = str(params)

    res = req.request(method='POST', url=url, data=param, headers=header, timeout=1000, proxies=None,)
    response =json.loads(res.content)
    result =   {"retCode": response["code"], "info": response["msg"], "respone": response}
    retCode, retMsg= result["retCode"], result["info"]

    b = phone+"," +message+ "," + str(retCode) + "," + retMsg.encode("utf-8") + ","  + sendTime  # 保存结果文件


    result_write(result_file, b)

def result_write(resultFile, b):
    result_file = open('%s' % (resultFile), "a")
    result_file.write('%s'%(b))
    result_file.write("\n")

# def run(logger,result_file,batchdict={},message={}):
#     for batch in  batchdict.items():
#         for phone  in batchdict[batch]:
#             phone = phone.strip()
#             content =message[batch].strip()
#             try:
#                 send(phone, content, result_file)
#                 logger.info("Sent --- %s :%s" % (phone, content))
#             except Exception as e:
#                 logger.error("unSend --- %s :%s \n" +e.__str__() % (phone, content))
#









if __name__ == '__main__':

    send('15032609451','sdsad','./d.txt')


    