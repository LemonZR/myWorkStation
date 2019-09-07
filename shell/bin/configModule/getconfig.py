#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import configparser

dbconfFile = "/echnlog/shell/conf/common.config"

def readdblist():
    try:
        cf = configparser.ConfigParser()
        cf.read(dbconfFile)
        return cf.items("db")
    except:
        print "error!"
    finally:
        cf.clear()

def readdbinfo(options):
    try:
        cf = configparser.ConfigParser()
        cf.read(dbconfFile)
        return cf.get('db', options)
    except:
        print "error!"
    finally:
        cf.clear()

def readcfg(section, options):
    try:
        cf = configparser.ConfigParser()
        cf.read(dbconfFile)
        return cf.get(section, options)
    except Exception, e:
        print "error [%s]", e
    finally:
        cf.clear()

def readsftpinfo(section):
    try:
        cf = configparser.ConfigParser()
        cf.read(dbconfFile)
        for kv in cf.items(section):
            if 'host' == kv[0]:
                host = kv[1]
            if 'port' == kv[0]:
                port = kv[1]
            if 'usrname' == kv[0]:
                usrname = kv[1]
            if 'password' == kv[0]:
                password = kv[1]
            if 'localpath' == kv[0]:
                localpath = kv[1]
            if 'downloadpath' == kv[0]:
                download = kv[1]
            if 'uploadpath' == kv[0]:
                upload = kv[1]
        return host, port, usrname, password, localpath, download, upload
    except:
        print "error!"
    finally:
        cf.clear()

# 根据手机号码获取数据库信息和表信息
# 手机号中间四位的尾号决定库，最后四位尾号决定表
def getdbinfobyphoneNo(phoneNo):
    return