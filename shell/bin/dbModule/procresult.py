#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import logging
import logging.config
import os
import sys
sys.path.append("..")  # 为了导入平级的模块
from dbModule import mysqlopt
from configModule import getconfig
from cmpModule import fileCmp
from toolsModule import commTools

# 读取差异文件进行处理
def procdifffile(difffile, retbyoptfile):
    logger = logging.getLogger("accountLog")
    try:
        if not os.path.exists(difffile):
            logger.warn(u"处理差异[%s]文件不存在!", difffile)
            return
        difffp = open(difffile, "rb+")
        retfp = open(retbyoptfile, "ab+")
        for line in difffp.readlines():
            if len(line.strip().split("|")) != 7:
                logger.error(u"error line [%s] from [%s]!", line, difffile)
                continue
            type, datestr, orderid, pw, loginno, prov, opt = line.strip().split("|")
            if type == "001" or type == "002":  # 暂时只处理这两类
                dblist, table = mysqlopt.getdbinfo(loginno)
                if len(dblist) == 0:
                    logger.error(u"error get db info by phoneNo [%s]!", loginno)
                    continue
                for dbinfo in dblist:
                    mysqlopt.updatetodb(dbinfo, table, mysqlopt.getcouponstype(difffile), pw, loginno, opt)
                    line = "%s|%s|%s|%s|%s|%s\n" % (datestr, orderid, pw, loginno, prov, opt)
                    retfp.write(line)
        difffp.close()
        retfp.close()
    except Exception, e:
        logger.error(u"procdifffile error [%s]!", e)

# 处理所有差异文件
def procalldiff(datestr, type = "all", prov = '999'):
    logger = logging.getLogger("accountLog")
    tmpPath = getconfig.readcfg('path', 'tmpcmpPath')
    if (type == 'all' or type == 'recharge'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "recharge/" + datestr + "/"
        if prov != "999":
            couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + prov + "_001.dat"
            difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
            logger.debug(u"开始处理差异文件[%s]!", difffile)
            procdifffile(difffile, retbyoptfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + provCode + "_001.dat"
                difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
                logger.debug(u"开始处理差异文件[%s]!", difffile)
                procdifffile(difffile, retbyoptfile)
    if (type == 'all' or type == 'flow'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "flow/" + datestr + "/"
        if prov != "999":
            couponsfile = dataPath + "account_flowDB_" + datestr + "_" + prov + "_001.dat"
            difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
            logger.debug(u"开始处理差异文件[%s]!", difffile)
            procdifffile(difffile, retbyoptfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_flowDB_" + datestr + "_" + provCode + "_001.dat"
                difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
                logger.debug(u"开始处理差异文件[%s]!", difffile)
                procdifffile(difffile, retbyoptfile)
    if (type == 'all' or type == 'commodity'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "commodity/" + datestr + "/"
        couponsfile = dataPath + "account_commodityDB_" + datestr + "_" + "9999999" + "_001.dat"
        difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
        logger.debug(u"开始处理差异文件[%s]!", difffile)
        procdifffile(difffile, retbyoptfile)
    if (type == 'all' or type == 'package'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "package/" + datestr + "/"
        couponsfile = dataPath + "account_packageDB_" + datestr + "_" + "999" + "_001.dat"
        difffile, resultfile, retbyoptfile = fileCmp.getresultfilename(tmpPath, couponsfile)
        logger.debug(u"开始处理差异文件[%s]!", difffile)
        procdifffile(difffile, retbyoptfile)

if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")