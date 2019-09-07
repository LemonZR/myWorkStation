#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import mysql.connector
import logging
import logging.config
import re
import sys
import os

sys.path.append("..")
from configModule import getconfig
from toolsModule import commTools

#根据手机号码获取数据库和表信息
def getdbinfo(phoneNo):
    logger = logging.getLogger("accountLog")
    dblist = []
    table = ""
    #判断是否手机号码，正则可以优化
    if re.match("\\d{11}",phoneNo):
        table = "td_pcard_info_" + str(int(phoneNo) % 10) #尾数取余得到表
        for dbcfg in getconfig.readdblist():  # 遍历所有库
            dbflag, dbinfo = dbcfg
            regex = "db" + "\\d" + "_" + str(int(phoneNo[3:7]) % 10)  #中间4位取余得到库，可能是列表
            if re.match(regex, dbflag):
                dblist.append(dbinfo)
    else:
        table = "td_pcard_info_u" #邮箱号码对应的表名
        for dbcfg in getconfig.readdblist():  # 遍历所有库
            dbflag, dbinfo = dbcfg
            if re.match("db\\d_u", dbflag):
                dblist.append(dbinfo)
    logger.debug(u"getdbinfo by [%s] result is table [%s] dblist [%s]", phoneNo, table, str(dblist))
    return dblist, table

# 根据文件名获取卡品类
def getcouponstype(busiOnlyfile):
    if busiOnlyfile.find("recharge") != -1:
        return "hf01"
    elif busiOnlyfile.find("flow") != -1:
        return "ll01"
    elif busiOnlyfile.find("commodity") != -1:
        return "sw01"
    elif busiOnlyfile.find("package") != -1:
        return "tc01"
    else:
        return ""

# 更新数据库信息
def updatetodb(dbinfo, table, type, pw, phoneNo, opt):
    logger = logging.getLogger("accountLog")
    host, port, db, usrname, dbpw = dbinfo.split(',')
    try:
        conn = mysql.connector.connect(host=host, port=port, user=usrname, password=dbpw, database=db, use_unicode=True)
        cursor = conn.cursor()
        updatesql = "update " + table + " set LIFECYCLE_ST = %s where PCARD_PASSWD = %s and BIND_NO = %s and \
        BUSI_TYPE=%s and LIFECYCLE_ST in (8,9,10)"
        input = (int(opt), pw, phoneNo, type)
        cursor.execute(updatesql, input)
        conn.commit()
        cursor.close()
        conn.close
    except Exception, e:
        logger.error(u"update mysql, error [%s]!", e)

# 从数据库查询数据
def getdatafromdb(dbinfo, table, type, pw, phoneNo):
    logger = logging.getLogger("accountLog")
    host, port, db, usrname, dbpw = dbinfo.split(',')
    values = None
    try:
        conn = mysql.connector.connect(host=host, port=port, user=usrname, password=dbpw, database=db, use_unicode=True)
        cursor = conn.cursor()
        selectsql = "select concat(ifnull(DATE_FORMAT(USE_DATE,'%Y%m%d'),''),'|',ifnull(USE_ORDER_ID,''),'|',\
        PCARD_PASSWD,'|',BIND_NO,'|',ifnull(ACCOUNT_FLAT,''),'|',LIFECYCLE_ST) from " + table + " where BUSI_TYPE=%s and \
        BIND_NO=%s and PCARD_PASSWD=%s"
        input = (type, phoneNo, pw)
        cursor.execute(selectsql, input)
        values = cursor.fetchone()  # 应该只能查询出一条
        cursor.close()
        conn.close
    except Exception, e:
        logger.error(u"get data from mysql, error [%s]!", e)
    return values

# 读取上一步形成的差异文件，补充数据
def replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile):
    logger = logging.getLogger("accountLog")
    # 从busiOnlyfile读取数据，逐行扫描补充差异数据，如果有则补充到samebusifile和samecouponsfile文件中
    # 如果没有则补充到couponsOnlyfile文件中，注意用追加模式ab+
    try:
        type = getcouponstype(busiOnlyfile)
        if not os.path.exists(busiOnlyfile):
            logger.warn(u"补充缺失数据[%s]不存在!", busiOnlyfile)
            return
        bofFP = open(busiOnlyfile, "rb+")
        cofFP = open(couponsOnlyfile, "ab+")
        sbfFP = open(samebusifile, "ab+")
        scfFP = open(samecouponsfile, "ab+")
        pwmap = {}
        for line in bofFP.readlines():
            paramlist = line.split("|")
            if len(paramlist) != 6:
                logger.error(u"error data [%s] while read [%s]!", line, busiOnlyfile)
                continue
            if paramlist[2] in pwmap:  # 判断这个卡密数据是否已经提取过了，如果提取过了就不要重复提取
                if pwmap[paramlist[2]] == 1:  # 查询过这个卡密并且有数据已经补充过了，可以跳过
                    continue
                else:
                    cofFP.write("004|" + line)  # 但是如果这个数据是重复的并且属于业务系统的，需要补充到差异文件中
            else:
                dblist, table = getdbinfo(paramlist[3])
                if len(dblist) == 0:
                    logger.error(u"error get db info by phoneNo [%s]!", paramlist[3])
                    continue
                for dbinfo in dblist:
                    ret = getdatafromdb(dbinfo, table, type, paramlist[2], paramlist[3])
                    if ret:  # 查询有结果需要补充数据
                        pwmap[paramlist[2]] = 1  # 添加卡密，避免二次提取
                        sbfFP.write(line)
                        dataline = ret[0] + "\n"  # 查询出来的数据没有换行，返回值是一个元组，只取一条，默认第一条即可
                        scfFP.write(dataline)
                        logger.debug(u"补充数据[%s]到[%s]！",ret, samecouponsfile)
                    else:
                        pwmap[paramlist[2]] = 0
                        cofFP.write("004|"+line)  # 确认是004差异
                        logger.debug(u"确认差异存在，[%s]!", line)
        bofFP.close()
        cofFP.close()
        sbfFP.close()
        scfFP.close()
    except Exception, e:
        logger.error(u"replenishdata error [%s]!", e)

# 处理全部差异文件
def procbusiOnlyfile(datestr, type = "all", prov = '999'):
    logger = logging.getLogger("accountLog")
    tmpPath = getconfig.readcfg('path', 'tmpcmpPath')
    if (type == 'all' or type == 'recharge'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "recharge/" + datestr + "/"
        if prov != "999":
            couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + prov + "_001.dat"
            busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
            logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
            replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + provCode + "_001.dat"
                busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
                logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
                replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)
    if (type == 'all' or type == 'flow'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "flow/" + datestr + "/"
        if prov != "999":
            couponsfile = dataPath + "account_flowDB_" + datestr + "_" + prov + "_001.dat"
            busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
            logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
            replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_flowDB_" + datestr + "_" + provCode + "_001.dat"
                busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
                logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
                replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)
    if (type == 'all' or type == 'commodity'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "commodity/" + datestr + "/"
        couponsfile = dataPath + "account_commodityDB_" + datestr + "_" + "9999999" + "_001.dat"
        busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
        logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
        replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)
    if (type == 'all' or type == 'package'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "package/" + datestr + "/"
        couponsfile = dataPath + "account_packageDB_" + datestr + "_" + "999" + "_001.dat"
        busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
        logger.debug(u"开始扫描[%s]补充数据!", busiOnlyfile)
        replenishdata(busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile)

if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")

    '''
    
    '''