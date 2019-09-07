#!/usr/bin/env python
# _*_ coding:utf-8 _*_


import time
import os
import logging
import logging.config

def getprovDict():
    return {'100': u'北京', '200': u'广东', '210': u'上海', '220': u'天津','230': u'重庆', \
		'240':u'辽宁','250':u'江苏','270':u'湖北','280':u'四川','290':u'陕西',\
		'311':u'河北','351':u'山西','371':u'河南','431':u'吉林','451':u'黑龙江',\
		'471':u'内蒙古','531':u'山东','551':u'安徽','571':u'浙江','591':u'福建',\
		'731':u'湖南','771':u'广西','791':u'江西','851':u'贵州','871':u'云南',\
		'891':u'西藏','898':u'海南','931':u'甘肃','951':u'宁夏','971':u'青海',\
		'991':u'新疆'	}

def gettimestr(fmt = "YYYYMMDD"):
    if fmt == "YYYYMMDD":
        return time.strftime("%Y%m%d", time.localtime())


# 生成第一次比对所需的文件名
def getcmpfilename(path, couponsfile):
    logger = logging.getLogger("accountLog")
    busiOnly = ""
    couponsOnly = ""
    samebusi = ""
    samecoupons = ""
    flist = os.path.basename(couponsfile).split("_")
    if flist[1].find("recharge") != -1:
        local = path + "recharge/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        busiOnly = local + "diff_rechargeOnly_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        couponsOnly = local + "diff_coupons2recharge_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samebusi = local + "same_recharge2coupons_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samecoupons = local + "same_rechargeDB_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("flow") != -1:
        local = path + "flow/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        busiOnly = local + "diff_flowOnly_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        couponsOnly = local + "diff_coupons2flow_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samebusi = local + "same_flow2coupons_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samecoupons = local + "same_flowDB_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("commodity") != -1:
        local = path + "commodity/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        busiOnly = local + "diff_commodityOnly_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        couponsOnly = local + "diff_coupons2commodity_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samebusi = local + "same_commodity2coupons_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samecoupons = local + "same_commodityDB_" + flist[2] + "_" + flist[3] + "_" + flist[4]
    elif flist[1].find("package") != -1:
        local = path + "package/" + flist[2] + "/"
        if not os.path.exists(local):
            os.makedirs(local)
        busiOnly = local + "diff_packageOnly_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        couponsOnly = local + "diff_coupons2package_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samebusi = local + "same_package2coupons_" + flist[2] + "_" + flist[3] + "_" + flist[4]
        samecoupons = local + "same_packageDB_" + flist[2] + "_" + flist[3] + "_" + flist[4]

    logger.debug("busiOnly = [%s], couponsOnly = [%s], samebusi = [%s], samecoupons = [%s]", busiOnly, couponsOnly, samebusi, samecoupons)
    return busiOnly, couponsOnly, samebusi, samecoupons

#todo 对于生成文件名，可考虑统一封装一下，入参是路径，类型，账期，省份，序号，后续优化
if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")
