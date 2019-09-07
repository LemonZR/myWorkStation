#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import os
import re
import sys
import time
import shutil
import logging
import logging.config


sys.path.append("..")  # 为了导入平级的模块
from configModule import getconfig
from dbModule import mysqlopt
from toolsModule import commTools


#检查文件命名规范
def checkbothfile(busifile, couponsfile):
    logger = logging.getLogger("accountLog")
    bfilelist = os.path.basename(busifile).split("_")
    cfilelist = os.path.basename(couponsfile).split("_")
    if len(bfilelist) != 5:
        logger.error("error file name [%s]!", busifile)
        return False
    if len(cfilelist) != 5:
        logger.error("error file name [%s]!", couponsfile)
        return False
    if (bfilelist[2] != cfilelist[2] or bfilelist[3] != cfilelist[3] or bfilelist[4] != cfilelist[4]):
        logger.error("error file name [%s] and [%s]!", busifile, couponsfile)
        return False
    return True

#检查文件是否存在
def filepathcheck(filepath):
    logger = logging.getLogger("accountLog")
    if not os.path.exists(filepath):
        logger.error("[%s] not exists!", filepath)
        return False
    filename = os.path.basename(filepath)
    try:
        retlist = filename.split(".")[0].split("_")
        return retlist[len(retlist)-3], retlist[len(retlist)-2], retlist[len(retlist)-1]
    except Exception, e:
        logger.error("readfile Exception: [%s]!", e)
        return False

#文件读取，返回一个set包括所有的卡密，一个dict卡密作为key，所有信息作为value
def readfile(filefullname):
    logger = logging.getLogger("accountLog")
    couponspw = set()  # 卡密剃重，保存到set集合
    filerecords = {}  # 按照卡密，保存对应的记录，注意对于业务系统的文件，同一个卡密可能存在多个记录，所以这个字典的value是个list
    try:
        for record in open(filefullname, "rb"):
            pwkey, infos = parseline(record)
            if (len(pwkey) > 0 and len(infos) > 0):
                if pwkey not in couponspw:
                    couponspw.add(pwkey)
                    filerecords[pwkey] = []  # 没有记录过卡密，就把字典初始化一下，空list
                else:
                    # 会存在重复的卡密记录
                    logger.info("repeat couponspw [%s] in file [%s]", pwkey, filefullname)
                filerecords[pwkey].append(infos) # 把卡密对应的记录，添加到list，对于一个卡密有多个记录的，这个list长度大于1
        return couponspw, filerecords
    except Exception, e:
        logger.error("readfile Exception: [%s]!", e)

#文件记录解析，返回卡密，和解析后的list
def parseline(line):
    logger = logging.getLogger("accountLog")
    couponspw = ""
    infos = []
    try:
        infos = line.strip().split("|")
        if len(infos) == 6:
            couponspw = infos[2]
    except Exception, e:
        logger.error("error parseline [%s]!", line)
    return couponspw, infos

#生成第二次比对所需文件名
def getresultfilename(path, couponsfile):
    logger = logging.getLogger("accountLog")
    difffile = ""
    resultfile = ""
    retbyoptfile = ""
    flist = os.path.basename(couponsfile).split("_")
    if flist[1].find("recharge") != -1:
        local = path + "recharge/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        difffile = local + "diff_coupons2recharge_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        resultfile = local + "result_coupons2recharge_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        retbyoptfile = local + "retbyopt_coupons2recharge_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("flow") != -1:
        local = path + "flow/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        difffile = local + "diff_coupons2flow_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        resultfile = local + "result_coupons2flow_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        retbyoptfile = local + "retbyopt_coupons2flow_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("commodity") != -1:
        local = path + "commodity/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        difffile = local + "diff_coupons2commodity_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        resultfile = local + "result_coupons2commodity_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        retbyoptfile = local + "retbyopt_coupons2commodity_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("package") != -1:
        local = path + "package/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        difffile = local + "diff_coupons2package_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        resultfile = local + "result_coupons2package_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        retbyoptfile = local + "retbyopt_coupons2package_" + flist[2] + "_" + flist[3] + "_" + flist[4]

    logger.debug("difffile = [%s], resultfile = [%s], retbyoptfile=[%s]", difffile, resultfile, retbyoptfile)
    return difffile, resultfile, retbyoptfile


# 输入2个文件，比较差异， 第一次比对，根据卡密比对
def cmpdiff(busifile, couponsfile):
    logger = logging.getLogger("accountLog")
    # 检查文件是否存在
    if not filepathcheck(busifile):
        return -1
    if not filepathcheck(couponsfile):
        return -2
    # 检查文件名是否符合要求
    if not checkbothfile(busifile, couponsfile):
        return -3
    #获取目录和待生成的文件名
    tmpPath = getconfig.readcfg('path', 'tmpcmpPath')
    if not os.path.exists(tmpPath):
        logger.error("config tmpcmpPath [%s] do not exists!", tmpPath)
        return -4
    busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)

    try:
        busiPW, busiInfos = readfile(busifile)
        couponsPW, couponsInfos = readfile(couponsfile)

        # 业务系统比卡券中心多的，处理后形成004差异
        busionly = list(set(busiPW).difference(set(couponsPW)))
        logger.debug(u"第一次比对[%s] 与 [%s] ， 业务系统多[%d]条记录！",busifile, couponsfile, len(busionly))
        fp = open(busiOnlyfile, "wb+")
        for pcardpw in busionly:
            for record in busiInfos[pcardpw]:
                line = "|".join(str(i) for i in record) + "\n"
                fp.write(line)
        fp.close()
        # 业务系统比卡券中心少的，005差异，需要业务系统确认后再处理
        # 文件记录时前缀增加005标记
        couponsonly = list(set(couponsPW).difference(set(busiPW)))
        logger.debug(u"第一次比对[%s] 与 [%s] ， 卡券中心多[%d]条记录！", busifile, couponsfile, len(couponsonly))
        fp = open(couponsOnlyfile, "wb+")
        for pcardpw in couponsonly:
            for record in couponsInfos[pcardpw]:
                line = "005|" + "|".join(str(i) for i in record) + "\n"
                fp.write(line)
        fp.close()
        # 双方一致的
        samelist = list(set(busiPW) & set(couponsPW))
        logger.debug(u"第一次比对[%s] 与 [%s] ， 双方一致[%d]条记录！", busifile, couponsfile, len(samelist))
        fp1 = open(samebusifile, "wb+")
        fp2 = open(samecouponsfile, "wb+")
        for pcardpw in samelist:
            for record in busiInfos[pcardpw]:
                line = "|".join(str(i) for i in record) + "\n"
                fp1.write(line)
            for record in couponsInfos[pcardpw]:
                line = "|".join(str(i) for i in record) + "\n"
                fp2.write(line)
        fp1.close()
        fp2.close()
    except IOError, e:
        logger.error("cmpdiff error [%s] and [%s]!",busifile, couponsfile)
        return -5

#处理所有需要比对的文件，充值、流量的分省，商品的不分省
def cmpallfist(datestr, type = "all", prov = '999'):
    logger = logging.getLogger("accountLog")
    if (type == 'all' or type == 'recharge'):
        ftppath = getconfig.readcfg('recharge_sftp', 'localpath') + "recharge/" + datestr + "/"
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "recharge/" + datestr + "/"  # 获取数据库导出数据路径
        if prov != "999":
            busifile = ftppath + "account_recharge2coupons_" + datestr + "_" + prov + "_001.dat"
            couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + prov + "_001.dat"
            logger.debug(u"开始比对充值中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, prov, busifile, couponsfile)
            cmpdiff(busifile, couponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                busifile = ftppath + "account_recharge2coupons_" + datestr + "_" + provCode + "_001.dat"
                couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + provCode + "_001.dat"
                logger.debug(u"开始比对充值中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, provCode, busifile, couponsfile)
                cmpdiff(busifile, couponsfile)
    if (type == 'all' or type == 'flow'):
        ftppath = getconfig.readcfg('flow_sftp', 'localpath') + "flow/" + datestr + "/"
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "flow/" + datestr + "/"
        if prov != "999":
            busifile = ftppath + "account_flow2coupons_" + datestr + "_" + prov + "_001.dat"
            couponsfile = dataPath + "account_flowDB_" + datestr + "_" + prov + "_001.dat"
            logger.debug(u"开始比对流量中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, prov, busifile, couponsfile)
            cmpdiff(busifile, couponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                busifile = ftppath + "account_flow2coupons_" + datestr + "_" + provCode + "_001.dat"
                couponsfile = dataPath + "account_flowDB_" + datestr + "_" + provCode + "_001.dat"
                logger.debug(u"开始比对流量中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, provCode, busifile, couponsfile)
                cmpdiff(busifile, couponsfile)
    if (type == 'all' or type == 'commodity'):
        ftppath = getconfig.readcfg('commodity_sftp', 'localpath') + "commodity/" + datestr + "/"
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "commodity/" + datestr + "/"
        busifile = ftppath + "account_commodity2coupons_" + datestr + "_" + "9999999" + "_001.dat"
        couponsfile = dataPath + "account_commodityDB_" + datestr + "_" + "9999999" + "_001.dat"
        logger.debug(u"开始比对商品中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, "9999999", busifile, couponsfile)
        cmpdiff(busifile, couponsfile)
    if (type == 'all' or type == 'package'):
        ftppath = getconfig.readcfg('package_sftp', 'localpath') + "package/" + datestr + "/"
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "package/" + datestr + "/"
        busifile = ftppath + "account_package2coupons_" + datestr + "_" + "999" + "_001.dat"
        couponsfile = dataPath + "account_packageDB_" + datestr + "_" + "999" + "_001.dat"
        logger.debug(u"开始比对商品中心和卡券中心文件，账期[%s]， 省份[%s], 文件[%s][%s]!", datestr, "999", busifile, couponsfile)
        cmpdiff(busifile, couponsfile)

# 针对同一个卡密进行比对
def cmprecord(pw, busiinfolist, couponsinfolist, difffile, resultfile, retbyoptfile):
    logger = logging.getLogger("accountLog")
    if not os.path.exists(difffile):
        logger.error("[%s] do not exists!", difffile)
        return -1
    # if not os.path.exists(resultfile): 这个文件可以不存在，第一次比较之前是不存在的39
    #    logger.error("[%s] do not exists!", resultfile)
    #    return -2
    datestr, prov, fileindex = filepathcheck(difffile)
    if len(busiinfolist) == 0:
        logger.error(u"第二次差异比对，业务系统记录为空, 账期[%s], 省份/商户[%s], 文件序号[%s], 卡密[%s]!", datestr, prov, fileindex, pw)
        return -3
    if len(couponsinfolist) != 1:
        logger.error(u"第二次差异比对，卡券提取数据重复, 同一卡密存在多条记录, 账期[%s], 省份/商户[%s], 文件序号[%s], 卡密[%s]", datestr, prov, fileindex, pw)
        return -4
    logger.debug(u"第二次差异比对, 账期[%s], 省份/商户[%s], 文件序号[%s], 卡密[%s]", datestr, prov, fileindex, pw)
    try:
        retfp = open(resultfile, "ab+")
        difffp = open(difffile, "ab+")
        optfp = open(retbyoptfile, "ab+")
        c_date, c_orderid, c_pw, c_loginno, c_prov, c_opt = couponsinfolist[0] # 卡券中心提取的数据应该只有一条，[对账账期,业务中心订单号,券码,登录号码,省份编码/商户ID,操作类型]
        for busiinfo in busiinfolist:
            b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt = busiinfo
            if b_pw != c_pw:
                logger.error(u"第二次比对，卡密不一致[%s]和[%s]!", pw, b_pw, c_pw)
                return -6
            if b_loginno != c_loginno:
                logger.error(u"第二次比对，卡密[%s], 登录号码不一致[%s]和[%s]!", pw, b_loginno, c_loginno)
                return -7
            if b_orderid == c_orderid:
                if b_opt == c_opt:  # 平账
                    line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                    retfp.write(line)
                    if b_date != c_date or b_loginno != c_loginno or b_prov != c_prov:
                        logger.warn(u"第二次比对, 卡密[%s]平账，附属信息不一致，卡券中心[%s|%s|%s]，业务中心[%s|%s|%s]", c_pw,
                                    c_date, c_loginno, c_prov, b_date, b_loginno, b_prov)
                else:
                    if c_opt == "8":  # 卡券处于释放状态，001差异
                        line = "001|%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                        difffp.write(line)
                    elif b_opt == "8":
                        if c_opt == "9":  # 卡券是锁定的，直接返销
                            dblist, table = mysqlopt.getdbinfo(b_loginno)
                            if len(dblist) == 0:
                                logger.error(u"error get db info by phoneNo [%s]!", b_loginno)
                                continue
                            for dbinfo in dblist:
                                mysqlopt.updatetodb(dbinfo, table, mysqlopt.getcouponstype(resultfile), pw, b_loginno, b_opt)
                                line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                                optfp.write(line)
                        elif c_opt == "10":  # 业务系统处于释放状态，卡券是使用的002差异
                            line = "002|%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                            difffp.write(line)
                        else:
                            logger.warn(u"第二次比对, 业务系统与卡券中心状态不一致， 账期[%s], 登录号码[%s], 卡密[%s], "
                                        u"订单号[%s], 业务系统订单状态[%s], 卡券中心订单状态[%s]!", \
                                        datestr, b_loginno, pw, b_orderid, b_opt, c_opt)
                            line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                            retfp.write(line)
                    else:  # 其他不一致情况，业务系统锁定卡券使用，或者业务系统使用卡券锁定，暂定记录为平账，同时告警
                        logger.warn(u"第二次比对, 业务系统与卡券中心状态不一致， 账期[%s], 登录号码[%s], 卡密[%s], "
                                    u"订单号[%s], 业务系统订单状态[%s], 卡券中心订单状态[%s]!", \
                                    datestr, b_loginno, pw, b_orderid, b_opt, c_opt)
                        line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                        retfp.write(line)
                        # 更新数据库，以业务系统状态为准
                        # dblist, table = mysqlopt.getdbinfo(b_loginno)
                        # if len(dblist) == 0:
                        #     logger.error("error get db info by phoneNo [%s]!", b_loginno)
                        #     continue
                        # for dbinfo in dblist:  # 可能存在多个库并存，所以需要循环
                        #    mysqlopt.updatetodb(dbinfo, table, mysqlopt.getcouponstype(resultfile), pw, b_loginno, b_opt)
                        # line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                        # optfp.write(line)  # 记录到更新后平账文件中
            else:  # 订单号不一致
                if b_opt == "8":  # 如果是释放状态则平账，后续账期还会对账
                    line = "%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                    retfp.write(line)
                else:  # 003差异
                    line = "003|%s|%s|%s|%s|%s|%s\n" % (b_date, b_orderid, b_pw, b_loginno, b_prov, b_opt)
                    difffp.write(line)
        retfp.close()
        difffp.close()
        optfp.close()
    except Exception, e:
        logger.error("cmprecord error [%s] and [%s], error [%s]!", busiinfolist, couponsinfolist, e)
        return -5
    return 0

# 第二次比对，卡密一致进行比对
def cmpsame(samebusifile, samecouponsfile):
    logger = logging.getLogger("accountLog")
    # 检查文件是否存在
    if not filepathcheck(samebusifile):
        return -1
    if not filepathcheck(samecouponsfile):
        return -2
    # 检查文件名是否符合要求
    if not checkbothfile(samebusifile, samecouponsfile):
        return -3
    # 获取差异文件名和结果文件名
    tmpPath = getconfig.readcfg('path', 'tmpcmpPath')
    if not os.path.exists(tmpPath):
        logger.error("config tmpcmpPath [%s] do not exists!", tmpPath)
        return -4
    difffile, resultfile, retbyoptfile = getresultfilename(tmpPath, samecouponsfile)

    try:
        # 如果结果文件已经存在需要先删除，避免脏数据
        if os.path.exists(resultfile):
            os.remove(resultfile)
        if os.path.exists(retbyoptfile):
            os.remove(retbyoptfile)
        busipw, busirecords = readfile(samebusifile)
        couponspw, couponsrecords = readfile(samecouponsfile)
        if len(busipw) != len(couponspw):
            logger.error("error [%s] and [%s] password not same!", samebusifile, samecouponsfile)
            return -5
        # 按卡密遍历，逐个比对
        for pw in couponspw:
            if cmprecord(pw, busirecords[pw], couponsrecords[pw], difffile, resultfile, retbyoptfile) != 0:
                logger.error("cmpdiff error [%s] and [%s]!", samebusifile, samecouponsfile)
    except IOError, e:
        logger.error("cmpdiff error [%s] and [%s], error [%s]!", samebusifile, samecouponsfile, e)
        return -6

def cmpallsecond(datestr, type = "all", prov = '999'):
    logger = logging.getLogger("accountLog")
    tmpPath = getconfig.readcfg('path', 'tmpcmpPath')
    if (type == 'all' or type == 'recharge'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "recharge/" + datestr + "/"
        if prov != "999":
            couponsfile = getconfig.readcfg('path', 'exportDataPath') + "account_rechargeDB_" + datestr + "_" + prov + "_001.dat"
            busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
            logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
            cmpsame(samebusifile, samecouponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_rechargeDB_" + datestr + "_" + provCode + "_001.dat"
                busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
                logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
                cmpsame(samebusifile, samecouponsfile)
    if (type == 'all' or type == 'flow'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "flow/" + datestr + "/"
        if prov != "999":
            couponsfile = dataPath + "account_flowDB_" + datestr + "_" + prov + "_001.dat"
            busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
            logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
            cmpsame(samebusifile, samecouponsfile)
        else:
            for provCode in commTools.getprovDict().keys():  # 遍历所有省份比对
                couponsfile = dataPath + "account_flowDB_" + datestr + "_" + provCode + "_001.dat"
                busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
                logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
                cmpsame(samebusifile, samecouponsfile)
    if (type == 'all' or type == 'commodity'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "commodity/" + datestr + "/"
        couponsfile = dataPath + "account_commodityDB_" + datestr + "_" + "9999999" + "_001.dat"
        busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
        logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
        cmpsame(samebusifile, samecouponsfile)
    if (type == 'all' or type == 'package'):
        dataPath = getconfig.readcfg('path', 'exportDataPath') + "package/" + datestr + "/"
        couponsfile = dataPath + "account_packageDB_" + datestr + "_" + "999" + "_001.dat"
        busiOnlyfile, couponsOnlyfile, samebusifile, samecouponsfile = commTools.getcmpfilename(tmpPath, couponsfile)
        logger.debug(u"开始第二次比对[%s]和[%s]!", samebusifile, samecouponsfile)
        cmpsame(samebusifile, samecouponsfile)

if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")
    cmpallfist("20180307")
