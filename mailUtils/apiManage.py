#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2017年05月11日
@author: xiongjing
"""




import os
import json
import requests
import random
from requests.adapters import HTTPAdapter


# 伪装请求头部中的浏览器类型User-Agent
userAgentList = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]


class HttpApiManage(object):
    def __init__(self, useid=None,  timeout=15):
        self.use_id = useid
        self.timeout = timeout

    # 采集时为避免被封IP，经常会使用代理
    def setProxies(self):
        ip = "172.16.9.20"
        port = 8088
        proxies = {"http": "http://%s:%s" % (ip, port), "https": "http://%s:%s" % (ip, port)}
        return proxies

    # 设置请求的超时时间
    def setTimeout(self, timeout):
        _timeout = timeout
        if type(_timeout) is int or type(_timeout) is float:
            self.timeout = _timeout
        elif type(_timeout) is tuple:
            self.timeout = _timeout
        else:
            timeout = self.timeout
        return self.timeout

    # 伪装请求头部X-Forwarded-For
    def xForwardedFor(self):
        return '229.72.%s.%s' % (random.randint(123, 225), random.randint(123, 225))

    # 伪装请求头部是采集时经常用的，我们可以用这个方法来隐藏
    def setHeaders(self):
        headers = {
            "Host": "shop.10086.cn",
            'User-Agent': random.choice(userAgentList),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "Content-type": "application/json",
            "X-Forwarded-For": self.xForwardedFor(),
            "Connection": "keep-alive"}
        return headers

    # 读取本地cookie信息
    def readCookie(self, login):
        try:
            if login == u"是":
                cookie_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Cache/%s_cookie.txt" % self.use_id)
            else:
                cookie_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Cache/tempCookie.txt")
            if os.path.exists(cookie_path):
                cookie = open(cookie_path, "rb").read()
            else:
                open(cookie_path, "w")
                cookie = open(cookie_path,"rb").read()
            if len(cookie) == 0:
                cookie = {}
            return eval(cookie)
        except Exception, e:
            return {}

    def getParams(self, data):
        if type(data) is dict:
            params = json.dumps(data, encoding="UTF-8")
        elif data is None:
            params = ""
        else:
            params = data.encode("utf-8")
        return params

    def startDriver(self, login):
        driver = requests.session()
        # 配置超时及重试次数
        driver.mount("http://", HTTPAdapter(max_retries=1))
        driver.mount('https://', HTTPAdapter(max_retries=1))
        driver.cookies.update(self.readCookie(login))
        driver.headers.update(self.setHeaders())
        return driver

    # 转换header类型
    def getHeader(self, header):
        if header is None:
            header = {"Content-type": "application/json"}
        elif type(header) is dict:
            header = header
        else:
            try:
                header = eval(header)
            except Exception,e:
                header = {"Content-type": "application/json"}
        return header

    # 发送请求
    def httpInvoke(self, method, url, login=None, data=None, header=None, _timeout=None,proxies=None):
        timeout = self.setTimeout(_timeout)
        params = self.getParams(data)
        header = self.getHeader(header)
        #proxies = self.setProxies()
        try:
            driver = self.startDriver(login)
            response = driver.request(method=method, url=url, data=params, headers=header, timeout=timeout, proxies=proxies)
            self.saveCookie(driver, login)
            return response
        except requests.exceptions.ConnectTimeout:
            response = "ConnectTimeout"
            return response
        except requests.exceptions.Timeout:
            response = "Timeout"
            return response
        except requests.exceptions.ConnectionError:
            response = "ConnectionError"
            return response
        except Exception,e:
            response = e
            return response

    # 保存cookie,类型为字典dict
    def saveCookie(self, driver,login):
        if login == u"是":
            cookie_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Cache/%s_cookie.txt" % self.use_id)
        else:
            cookie_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "Cache/tempCookie.txt")
        if os.path.exists(cookie_path):
            cookie_file = open(cookie_path, "rb+")
        else:
            open(cookie_path, "w")
            cookie_file = open(cookie_path, "rb+")
        cookie_dict = requests.utils.dict_from_cookiejar(driver.cookies)
        if len(cookie_dict) == 0:
            return
        else:
            session = self.readCookie(login)
            session.update(cookie_dict)
            cookie_file.write(str(session))
        cookie_file.close()


