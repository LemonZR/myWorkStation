#!/usr/bin/env python
# -*- coding: utf8 -*-
import sys

reload(sys)

sys.setdefaultencoding('utf8')
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
        self.nick = u'小特特'
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
from bottle import template
import time
import datetime
def get_html(result_filename):
    now_time = datetime.datetime.now()
    the_day = (now_time + datetime.timedelta(days=0)).strftime('%Y-%m-%d')

    nowdate =time.strftime("%Y/%m/%d/ %H:%M:%S", time.localtime())
    items = genHtmlArgs(result_filename)
    template_demo=open("/echnweb/zhangrui/tehuiri_recharge.html").read()
    html = template(template_demo.decode('utf8'),tag_day=the_day,nowdate=nowdate,items=items)
    return html

def genHtmlArgs(result_filename):
    a= []
    items = []
    with open('%s'%result_filename) as file:
        for line in file:
            lines = line.split(',')
            items.append(lines)
    return items
if __name__ == "__main__":
    a =sys.argv[2].encode("gbk")
    attachments = [unicode(a,'gbk')]
    content=get_html(sys.argv[1])
    #to='zhangrui_miso@si-tech.com.cn'
    #to='service_pro@si-tech.com.cn,mengly@si-tech.com.cn,service_shop@si-tech.com.cn,liujh_miso@si-tech.com.cn'
    emaiMange(to=to).moblieSend(u"特惠日充值活动监控", content,attachment=attachments)