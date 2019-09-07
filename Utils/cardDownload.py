#!/usr/bin/env python
# -*- coding: cp936 -*-

# 2017校园青春卡”名单获取

import requests
from datetime import timedelta, datetime


# 获取昨天日期
def getYesterday():
    yesterday = datetime.today() + timedelta(-1)
    # 文件名
    #flie_name = yesterday.strftime('%Y%m%d')
    flie_name = "20170gfh"
    return flie_name


# 下载青春卡号码数据文件
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
        print  u"号码下载失败"


if __name__ == "__main__":
    cardDownload()
