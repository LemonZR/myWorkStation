#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import sys
import os
import logging
import logging.config
import shutil

sys.path.append("..")  # 为了导入平级的模块
from configModule import getconfig
from toolsModule import commTools


# 合并文件
def getcatCmd(type, path, datestr, provCode):
    if type == "recharge":
        finialfile = path + "account_rechargeDB_" + datestr + "_" + provCode + "_001.dat"
        return "cat " + path + "tmp_account_rechargeDB_*" + datestr + "_" + provCode + "_001.dat > " + finialfile
    elif type == "flow":
        finialfile = path + "account_flowDB_" + datestr + "_" + provCode + "_001.dat"
        return "cat " + path + "tmp_account_flowDB_*" + datestr + "_" + provCode + "_001.dat > " + finialfile
    elif type == "commodity":
        finialfile = path + "account_commodityDB_" + datestr + "_" + provCode + "_001.dat"
        return "cat " + path + "tmp_account_commodityDB_*" + datestr + "_" + provCode + "_001.dat > " + finialfile
    elif type == "package":
        finialfile = path + "account_packageDB_" + datestr + "_" + provCode + "_001.dat"
        return "cat " + path + "tmp_account_packageDB_*" + datestr + "_" + provCode + "_001.dat > " + finialfile
    else:
        return ""


# 删除无用文件
def getdelCmd(type, path, datestr, provCode):
    if type == "recharge":
        return "rm " + path + "tmp_account_rechargeDB_*" + datestr + "_" + provCode + "_001.dat"
    elif type == "flow":
        return "rm " + path + "tmp_account_flowDB_*" + datestr + "_" + provCode + "_001.dat"
    elif type == "commodity":
        return "rm " + path + "tmp_account_commodityDB_*" + datestr + "_" + provCode + "_001.dat"
    elif type == "package":
        return "rm " + path + "tmp_account_packageDB_*" + datestr + "_" + provCode + "_001.dat"
    else:
        return ""


# 获取导出文件
def getexportfile(type, path, dbflag, index, datestr, provCode):
    if type == "recharge":
        return path + "tmp_account_rechargeDB_" + dbflag + "_" + index + "_" + datestr + "_" + provCode + "_001.dat"
    elif type == "flow":
        return path + "tmp_account_flowDB_" + dbflag + "_" + index + "_" + datestr + "_" + provCode + "_001.dat"
    elif type == "commodity":
        return path + "tmp_account_commodityDB_" + dbflag + "_" + index + "_" + datestr + "_" + provCode + "_001.dat"
    elif type == "package":
        return path + "tmp_account_packageDB_" + dbflag + "_" + index + "_" + datestr + "_" + provCode + "_001.dat"
    else:
        return ""


# 获取执行sql
def getsqlCmd(type, mysql, host, port, usrname, pw, db, index, datestr, provCode, outfile):
    if (type == "recharge"):
        return mysql + " -h " + host + " -P " + port + " -u " + usrname + " -p" + pw + " -D " + db + " -N " + \
            " -e " + "\"select concat(ifnull(DATE_FORMAT(USE_DATE,'%Y%m%d'),''),'|',ifnull(USE_ORDER_ID,''),'|',\
            PCARD_PASSWD,'|',BIND_NO,'|',ifnull(ACCOUNT_FLAT,''),'|',LIFECYCLE_ST) from " + \
            "td_pcard_info_" + index + " where BUSI_TYPE='hf01' and USE_DATE=" + datestr + " and ACCOUNT_FLAT=" + "\"" + provCode + "\"" + "\"" +\
            " > " + outfile
    elif (type == "flow"):
        return mysql + " -h " + host + " -P " + port + " -u " + usrname + " -p" + pw + " -D " + db + " -N " + \
            " -e " + "\"select concat(ifnull(DATE_FORMAT(USE_DATE,'%Y%m%d'),''),'|',ifnull(USE_ORDER_ID,''),'|',\
            PCARD_PASSWD,'|',BIND_NO,'|',ifnull(ACCOUNT_FLAT,''),'|',LIFECYCLE_ST) from " + \
            "td_pcard_info_" + index + " where BUSI_TYPE='ll01' and USE_DATE=" + datestr + " and ACCOUNT_FLAT=" + "\"" + provCode + "\"" + "\"" +\
             " > " + outfile
    elif (type == "commodity"):
        return mysql + " -h " + host + " -P " + port + " -u " + usrname + " -p" + pw + " -D " + db + " -N " + \
            " -e " + "\"select concat(ifnull(DATE_FORMAT(USE_DATE,'%Y%m%d'),''),'|',ifnull(USE_ORDER_ID,''),'|',\
           PCARD_PASSWD,'|', BIND_NO,'|',ifnull(ACCOUNT_FLAT,''),'|',LIFECYCLE_ST) from " + \
            "td_pcard_info_" + index + " where BUSI_TYPE='sw01' and USE_DATE=" + datestr + "\"" + " > " + outfile
    elif (type == "package"):
        return mysql + " -h " + host + " -P " + port + " -u " + usrname + " -p" + pw + " -D " + db + " -N " + \
            " -e " + "\"select concat(ifnull(DATE_FORMAT(USE_DATE,'%Y%m%d'),''),'|',ifnull(USE_ORDER_ID,''),'|',\
           PCARD_PASSWD,'|', BIND_NO,'|',ifnull(ACCOUNT_FLAT,''),'|',LIFECYCLE_ST) from " + \
            "td_pcard_info_" + index + " where BUSI_TYPE='tc01' and USE_DATE=" + datestr + "\"" + " > " + outfile
    else:
        return ""


def connandexport(type, path, datestr, prov):
    mysql = getconfig.readcfg('mysql', 'execpath')  # 获取mysql命令执行路径
    for dbcfg in getconfig.readdblist():  # 遍历所有库
        dbflag, dbinfo = dbcfg
        host, port, db, usrname, pw = dbinfo.split(',')
        if dbflag == "db0_u":
            exportData(type, mysql, host, port, usrname, pw, db, path, dbflag, "u", datestr, prov)
        else:
            for i in range(0, 10):
                exportData(type, mysql, host, port, usrname, pw, db, path, dbflag, str(i), datestr, prov)


def exportData(type, mysql, host, port, usrname, pw, db, path, dbflag, dbindex, datestr, prov):
    logger = logging.getLogger("accountLog")
    # 连接数据库导出
    outfile = getexportfile(type, path, dbflag, dbindex, datestr, prov)
    cmd = getsqlCmd(type, mysql, host, port, usrname, pw, db, dbindex, datestr, prov, outfile)
    logger.debug(u"exec cmd = [%s]!", cmd)
    os.system(cmd)


def catandclear(type, path, datestr, prov):
    logger = logging.getLogger("accountLog")
    cmd = getcatCmd(type, path, datestr, prov)
    logger.debug(u"exec cmd = [%s]!", cmd)
    os.system(cmd)
    cmd = getdelCmd(type, path, datestr, prov)
    logger.debug(u"exec cmd = [%s]!", cmd)
    os.system(cmd)


def getAccountData(datestr, type='all', prov='999'):
    logger = logging.getLogger("accountLog")
    if type == 'all' or type == 'recharge':
        logger.debug(u"开始导出充值相关数据，账期[%s]，省份[%s]", datestr, prov)
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "recharge/" + datestr + "/"
        if prov == '999':
            if os.path.exists(dataPath):
                shutil.rmtree(dataPath)
            os.makedirs(dataPath)
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份
                connandexport('recharge', dataPath, datestr, provCode)
                catandclear('recharge', dataPath, datestr, provCode)
        else:
            if not os.path.exists(dataPath):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                os.makedirs(dataPath)
            connandexport('recharge', dataPath, datestr, prov)
            catandclear('recharge', dataPath, datestr, prov)
    if type == 'all' or type == 'flow':
        logger.debug(u"开始导出流量相关数据，账期[%s]，省份[%s]",datestr, prov)
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "flow/" + datestr + "/"
        if prov == '999':
            if os.path.exists(dataPath):
                shutil.rmtree(dataPath)
            os.makedirs(dataPath)
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份
                connandexport('flow', dataPath, datestr, provCode)
                catandclear('flow', dataPath, datestr, provCode)
        else:
            if not os.path.exists(dataPath):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                os.makedirs(dataPath)
            connandexport('flow', dataPath, datestr, prov)
            catandclear('flow', dataPath, datestr, prov)
    if type == 'all' or type == 'commodity':
        logger.debug(u"开始导出商品相关数据，账期[%s]，省份[%s]",datestr, prov)
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "commodity/" + datestr + "/"
        if os.path.exists(dataPath):
            shutil.rmtree(dataPath)
        os.makedirs(dataPath)
        connandexport('commodity', dataPath, datestr, '9999999')
        catandclear('commodity', dataPath, datestr, '9999999')
    if type == 'all' or type == 'package':
        logger.debug(u"开始导出套餐相关数据，账期[%s]，省份[%s]",datestr, prov)
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "package/" + datestr + "/"
        if os.path.exists(dataPath):
            shutil.rmtree(dataPath)
        os.makedirs(dataPath)
        connandexport('package', dataPath, datestr, '999')
        catandclear('package', dataPath, datestr, '999')


if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")
    getAccountData('20180211')
