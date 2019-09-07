#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
@author: xiongjing
@edit by hesc
@then edit by zhangrui_miso
"""


import new_smtplib
from pyapilog import pyapilog
import runmode_config
import datetime

import os
import platform
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate

import sys
timestr="tehuiri"
import os
def get_file(weekfile_dir,timestr):
    reslist = []
    flist = os.popen('ls %s/*%s* 2> /dev/null'%(weekfile_dir,timestr))
    filesList = flist.readlines()
    for i in filesList:
        reslist.append(i.strip())
    return reslist
firstarg = sys.argv[1]

resfilename = '%s'%firstarg

def get_host():
    if platform.system() == "Windows":
        host = runmode_config.getconfig("mail.conf", "SMTP", "windows_host")
    else:
        host = runmode_config.getconfig("mail.conf", "SMTP", "linux_host")
    return host

def get_user():
    return runmode_config.getconfig("mail.conf", "SMTP", "zhangrui")

def get_pwd():
    return runmode_config.getconfig("mail.conf", "SMTP", "zhangrui_pwd")

def get_port():
    return runmode_config.getconfig("mail.conf", "SMTP", "port")

def get_tolist(to):
    if to is None:
        return runmode_config.getconfig("mail.conf", "SMTP", "to_addrs").split(",")
    else:
        return to

def get_encrypt():
    return runmode_config.getconfig("mail.conf", "SMTP", "encrypt")


class emaiMange(object):
    def __init__(self, to=None):
        self.mail_host = get_host()
        self.login_user = get_user()
        self.login_pwd = get_pwd()
        self.mail_port = get_port()
        self.to_list = get_tolist(to)
        self.encrypt = get_encrypt()

    def moblieSend(self, subject, content, files=[]):
        try:
            fro = self.login_user
            server = {'name': self.mail_host, 'user': self.login_user, 'passwd': self.login_pwd, 'port': self.mail_port}
            msg = MIMEMultipart()
            msg['From'] = fro
            msg['Subject'] = subject
            msg['To'] = COMMASPACE.join(self.to_list)
            msg['Date'] = formatdate(localtime=True)
            msg.attach(MIMEText(content, 'html', _charset='utf8'))
            #msg.attach(MIMEText(content, _charset='utf-8'))
            for f in files:
                if f == None:
                    pass
                else:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(open(f, 'rb').read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f).encode("utf8"))
                    msg.attach(part)
            if self.encrypt == 1:
                smtp = smtplib.SMTP(server['name'], server['port'])
                smtp.starttls()
                smtp.ehlo()
                smtp.login(server['user'], server['passwd'])
                smtp.sendmail(fro, self.to_list, msg.as_string())
                smtp.close()
            else:
                smtp = smtplib.SMTP(server['name'], server['port'])
                smtp.login(server['user'], server['passwd'])
                smtp.sendmail(fro, self.to_list, msg.as_string())
                smtp.close()
            pyapilog().info(u'------------------------------ÓÊ¼þ·¢ËÍ³É¹¦------------------------------')
        except Exception, e:
            pyapilog().error(u'------------------------------ÓÊ¼þ·¢ËÍÊ§°Ü------------------------------')
            pyapilog().exception("Exception Logged")

    def sitechSend(self, subject, content, files=[]):
        try:
            fro = self.login_user
            msg = MIMEMultipart()
            msg['From'] = fro
            msg['Subject'] = subject
            msg['To'] = COMMASPACE.join(self.to_list)
            msg['Date'] = formatdate(localtime=True)
            msg.attach(MIMEText(content, _charset='utf-8'))
            for f in files:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(open(f, 'rb').read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f).encode("utf-8"))
                msg.attach(part)
            server = new_smtplib.SMTP(self.mail_host, self.mail_port)
            server.set_debuglevel(0)
            server.login(self.login_user, self.login_pwd)
            server.sendmail(fro, self.to_list, msg.as_string())
            server.quit()
            pyapilog().info(u'哦了')
        except Exception, e:
            pyapilog().error(u'这有毛病')
            pyapilog().exception("Exception Logged")

from bottle import template
import time
# def get_html(items,day_rate):
#     nowdate = time.strftime("%Y/%m/%d/ %H:%M:%S", time.localtime())
#     template_demo = open(os.path.dirname(os.getcwd())+"/commonality/html/hb110.html").read()
#     html = template(template_demo.decode('gbk'), dayrate=day_rate,yesterday=nowdate, nowdate=nowdate,items=items)
#     return html

def get_html():
    now_time = datetime.datetime.now()
    the_day = (now_time + datetime.timedelta(days=0)).strftime('%Y-%m-%d')


    nowdate =time.strftime("%Y/%m/%d/ %H:%M:%S", time.localtime())
    items = genHtmlArgs()
    template_demo=open("/echnweb/tongji_data/tools/commonality/html/tehuiri_recharge.html").read()
    html = template(template_demo.decode('utf8'),tag_day=the_day,nowdate=nowdate,items=items)
    return html

def genHtmlArgs():
    a= []
    items = []
    with open('%s'%resfilename) as file:
        for line in file:
            lines = line.split(',')
            items.append(lines)
    return items


if __name__ == "__main__":
    #to = ['hesc@si-tech.com.cn','service_shop@si-tech.com.cn','service_pro@si-tech.com.cn','hanyf@si-tech.com.cn','mengly@si-tech.com.cn','mayz@si-tech.com.cn']
    to = ['zhangrui_miso@si-tech.com.cn',]
    smail = emaiMange(to=to)
    fileName = '%s'%resfilename
    #weekfile_dir = "/echnweb/tongji_data/tools/querytool/data/tehuiri/201908"
    #reslist = get_file(weekfile_dir,timestr)
    #appendixes = reslist
    appendixes= []
    content = get_html()
    smail.moblieSend(u"特惠日充值活动报告", content=content,files=appendixes)