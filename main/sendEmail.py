#coding:utf8


import os
import sys
import  time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from datetime import timedelta
from bottle import template
from mailUtils import sendMail

from Utils import getDate

yesterday =getDate.getYesterday()
def get_html(resData,actName):

    nowdate = time.strftime("%Y/%m/%d/ %H:%M:%S", time.localtime())
    phoneFileName = actName + yesterday
    resultFile = '%s/%s.txt' % (resData, phoneFileName)
    sum, True, False, success_rate = genDictByFalse(resultFile)
    html = "%s/html/%s.html"%(BASE_DIR, actName)
    template_demo = open(html).read()
    html = template(template_demo.decode('utf-8',errors= 'ignore'), yesterday=yesterday, nowdate=nowdate, sum=sum, True=True, False=False,
                    success_rate=success_rate)
    return html


def genDictByFalse(resultFile):
    a = []
    with open(resultFile) as file:
        for lines in file:
            line = lines.replace("\n", "")
            list = line.split(',')
            b = ",".join([list[-2]])
            a.append(b)
    myset = set(a)
    False = 0
    True = 0
    for item in myset:
        if item == "False":
            False = a.count(item)
        else:
            True = a.count(item)
    sum = False + True
    success_rate = float(format(float(True) / float(False + True), '.2f')) * 100
    return sum, True, False, success_rate


def sendEmail(logger,resData,actName):
    """
    to = ["15807148106@139.com", "13720154038@139.com", "kanying@si-tech.com.cn", "xiongjing@si-tech.com.cn",
          'guoxx@si-tech.com.cn', 'xiecui@si-tech.com.cn', 'xiehya@si-tech.com.cn', 'service_shop@si-tech.com.cn',
          'zhangrui_miso@si-tech.com.cn', 'liuzw@si-tech.com.cn', 'lihl@si-tech.com.cn', "hesc@si-tech.com.cn"]
    """
    to  =['zhangrui_miso@si-tech.com.cn',"guoxx@si-tech.com.cn","wangmy@si-tech.com.cn","hesc@si-tech.com.cn","xiecui@si-tech.com.cn","liuzw@si-tech.com.cn","xiehya@si-tech.com.cn","lihl@si-tech.com.cn","xiongjing@si-tech.com.cn","mengly@si-tech.com.cn"]
    smail = sendMail.emailMange(logger=logger,to=to)


    phoneFileName = actName + yesterday
    resultFile = '%s/%s.txt' % (resData, phoneFileName)
    print "结果文件："+resultFile
    files = [resultFile]
    content = get_html(resData,actName)
    try:
        smail.moblieSend(u"75元话费大壕送，随时畅聊不停歇@%s" % (phoneFileName), content=content, files=files)
        print ("邮件发送成功")
    except Exception as e:
        print e
        print "邮件发送失败"

if __name__ == "__main__":
    print yesterday