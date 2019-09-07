#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2017年05月14日
@author: xiongjing
"""

import my_smtplib
import os, sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from conf import runmode_config, config
import platform
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from Utils import printLog, createPath, getDate
# by zhangrui  为了给邮件加昵称
from email.header import Header
from email.utils import parseaddr, formataddr


# 获取邮箱主机域名或者IP
def get_host():
    if platform.system() == "Windows":
        host = runmode_config.getconfig("mail.conf", "SMTP", "windows_host")
    else:
        host = runmode_config.getconfig("mail.conf", "SMTP", "linux_host")
    return host


# 获取邮箱用户名
def get_nick():
    return runmode_config.getconfig("mail.conf", "SMTP", "nick")


# by zhangrui
def get_from_addr():
    return runmode_config.getconfig("mail.conf", "SMTP", "from_addr")


# 获取邮箱密码
def get_pwd():
    return runmode_config.getconfig("mail.conf", "SMTP", "login_pwd")


# 获取邮箱端口
def get_port():
    return runmode_config.getconfig("mail.conf", "SMTP", "port")


# 获取收件人列表list
def get_tolist(to):
    if to is None:
        return runmode_config.getconfig("mail.conf", "SMTP", "to_addrs").split(",")
    else:
        return to


def get_encrypt():
    return runmode_config.getconfig("mail.conf", "SMTP", "encrypt")


# by zhangrui
def get_logger(logger):
    if logger is None:
        loggerDate = getDate.getToday()
        log_dir = createPath.mkdir(config.log_dir)
        got_logger = printLog.printLog(loggerDate, '', log_dir)
        return got_logger
    else:
        return logger


class emailMange(object):
    def __init__(self, logger=None, to=None):
        self.mail_host = get_host()
        self.nick = get_nick()
        self.from_addr = get_from_addr()
        self.login_pwd = get_pwd()
        self.mail_port = get_port()
        self.to_list = get_tolist(to)
        self.encrypt = get_encrypt()
        # by zhangrui
        self.logger = get_logger(logger)

    # subject:邮件主题;content:邮件正文;files:附件，list类型
    def moblieSend(self, subject, content, files=[]):
        try:
            # h = Header(str(self.nick),'utf-8')
            # h.append(str(self.from_addr),'ascii')

            logger = self.logger
            server = {'name': self.mail_host, 'user': self.from_addr, 'passwd': self.login_pwd, 'port': self.mail_port}
            msg = MIMEMultipart()
            #  from nick
            msg['From'] = formataddr([Header(self.nick, 'utf-8').encode(), self.from_addr])
            msg['Subject'] = subject
            msg['To'] = COMMASPACE.join(self.to_list)
            msg['Date'] = formatdate(localtime=True)
            msg.attach(MIMEText(content, 'html', _charset='gbk'))
            # msg.attach(MIMEText(content, _charset='utf-8'))
            for f in files:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(open(f, 'rb').read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="%s"' % os.path.basename(f).encode("utf-8"))
                msg.attach(part)
            if self.encrypt == 1:
                smtp = smtplib.SMTP(server['name'], server['port'])
                smtp.starttls()
                smtp.ehlo()
                smtp.login(server['user'], server['passwd'])
                smtp.sendmail(self.from_addr, self.to_list, msg.as_string())
                smtp.close()
            else:
                smtp = smtplib.SMTP(server['name'], server['port'])
                smtp.login(server['user'], server['passwd'])
                smtp.sendmail(self.from_addr, self.to_list, msg.as_string())
                smtp.close()

            logger.info(u'------------------------------邮件发送成功------------------------------')
        except Exception, e:
            logger = self.logger
            logger.error(u'------------------------------邮件发送失败------------------------------')
            print e

    # si-tech内网邮箱发送邮件
    def sitechSend(self, subject, logger, content, files=[]):
        try:
            fro = self.from_addr
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
                part.add_header('Content-Disposition',
                                'attachment; filename="%s"' % os.path.basename(f).encode("utf-8"))
                msg.attach(part)
            server = my_smtplib.SMTP(self.mail_host, self.mail_port)
            server.set_debuglevel(0)
            server.login(self.from_addr, self.login_pwd)
            server.sendmail(fro, self.to_list, msg.as_string())
            server.quit()
            logger.info(u'------------------------------邮件发送成功------------------------------')
        except Exception, e:
            logger.error(u'------------------------------邮件发送失败------------------------------')
            print e


if __name__ == "__main__":
    print get_logger(logger='')
