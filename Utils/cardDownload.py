#!/usr/bin/env python
# -*- coding: cp936 -*-

# 2017У԰�ഺ����������ȡ

import requests
from datetime import timedelta, datetime


# ��ȡ��������
def getYesterday():
    yesterday = datetime.today() + timedelta(-1)
    # �ļ���
    #flie_name = yesterday.strftime('%Y%m%d')
    flie_name = "20170gfh"
    return flie_name


# �����ഺ�����������ļ�
def cardDownload():
    flie_name = getYesterday()
    url = "http://10.255.254.108:12000/download/youngcard/%s.dat" % (flie_name)
    resp = requests.get(url,stream=True)
    if resp.status_code == 200:
        with open("./TestDate/%s.dat" % flie_name,'wb') as f:
            f.write(resp.content)
        f.close()
    else:
        print resp.status_code
        print  u"��������ʧ��"


if __name__ == "__main__":
    cardDownload()
