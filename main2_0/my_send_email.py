#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Created on 2017年05月14日
@author: xiongjing
"""

import time
import os,sys
import platform
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate,formataddr


class emaiMange(object):

    def __init__(self, to=None, cc=None):
        self.nick = u'流量对账'
        self.smtpuser = '15032609451@139.com'
        self.smtppwd = '250bushibaichi@'
        self.smtpport = '25'
        self.smptencrypt = 0
        if platform.system() == "Windows":
            self.smtpserver = 'mail.139.com'
        else:
            self.smtpserver = '221.176.9.170'
        if to is None:
            self.listTo = 'zhangrui_miso@si-tech.com.cn'
        else:
            self.listTo = to.split(",")
        if cc is None:
            self.listCc = []
        else:
            self.listCc = cc.split(",")

    # subject:邮件主题;content:邮件正文;files:附件，list类型
    def moblieSend(self, subject, content, strMsgHtml=None, listImagePath=None, attachment=[]):
        retry = 0
        while retry < 3:
            try:
                msg = MIMEMultipart()
                msg['From'] = formataddr([self.nick.encode('gbk'), self.smtpuser  ])
                msg['Subject'] = subject
                msg['To'] = COMMASPACE.join(self.listTo)
                msg['Cc'] = COMMASPACE.join(self.listCc)
                msg['Date'] = formatdate(localtime=True)
                msg.attach(MIMEText(content, 'html', _charset='utf8'))
                # msg.attach(MIMEText(content, _charset='utf-8'))
                # 判断是否添加附件
                if len(attachment):
                    for file in attachment:
                        print "file :" +file
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(open(file, 'rb').read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            'attachment; filename="%s"' % os.path.basename(file).encode("gbk")
                        )
                        msg.attach(part)

                if self.smptencrypt == 1:
                    smtp = smtplib.SMTP(self.smtpserver, self.smtpport)
                    smtp.starttls()
                    smtp.ehlo()
                    smtp.login(self.smtpuser, self.smtppwd)
                    smtp.sendmail(self.smtpuser, self.listTo, msg.as_string())
                    smtp.close()
                else:
                    smtp = smtplib.SMTP('221.176.9.170', '25')
                    smtp.login('15032609451@139.com', '250bushibaichi@')
                    smtp.sendmail('15032609451@139.com', self.listTo, msg.as_string())
                    smtp.close()

                print(u'------------------------------邮件发送成功------------------------------')
                break

            except Exception as e:
                retry += 1
                time.sleep(0.5 * retry)
                if retry == 2:
                    print(u'------------------------------邮件发送失败------------------------------')
                    print ("Exception Logged",e)
                    return False


if __name__ == "__main__":
    a =sys.argv[2].encode("gbk")
    date_s=sys.argv[3].encode("gbk")
    attachments = [unicode(a,'gbk')]
    content=unicode(sys.argv[1]).encode("gbk")

    #emaiMange(to='service_pro@si-tech.com.cn').moblieSend(u"未对账省份@"+date_s.encode("gbk"), content,attachment=attachments)
    emaiMange(to='zhangrui_miso@si-tech.com.cn').moblieSend(u"未对账省份@"+date_s.encode("gbk"), content,attachment=attachments)