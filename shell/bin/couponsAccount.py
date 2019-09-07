#!/usr/bin/env python
# _*_ coding:utf-8 _*_

import sys
import logging
import getopt

sys.path.append("..")
from ftpModule import sftpTools
from cmpModule import fileCmp
from dbModule import mysqlopt
from dbModule import procresult
from dbModule import dataExport

def usage():
    print u"操作命令："
    print u"正常对账: couponsAccount -d 20180315 -s 1"
    print u"分中心对账: couponsAccount -d 20180315 -s 1 -t recharge/flow/commodity/package"
    print u"分中心分省对账: couponsAccount -d 20180315 -s 1 -t recharge/flow/commodity/package -p 351/100/210"
    print u"执行对账的某一步: 携带 -s 参数， 用数字1-8表示" \
          u"第一步ftp下载各业务中心文件" \
          u"第二步从卡券中心数据库导出数据" \
          u"第三步第一次比对文件只比较卡密是否一致" \
          u"第四步补充缺失的数据" \
          u"第五步第二次比对输出各类差异文件" \
          u"第六步差异确认（目前人工处理）" \
          u"第七步扫描结果文件更新卡券状态（返销或者其他状态同步）" \
          u"第八步清理所有临时文件"

if __name__ == '__main__':
    try:
        logging.config.fileConfig("/echnlog/shell/conf/log.config")
        logger = logging.getLogger("accountLog")
        options, args = getopt.getopt(sys.argv[1:], "hd:t:p:s:")
    except getopt.GetoptError, e:
        print "error [%s]!" % e
        sys.exit()

    logger.debug(u"开始解析入参")
    datestr = ""
    type = "all"
    prov = "999"
    step = "all"
    for name, value in options:
        if name == "-h":
            usage()
            sys.exit()
        if name == "-d":
            datestr = value
        if name == "-t":
            type = value
        if name == "-p":
            prov = value
        if name == "-s":
            step = value

    if len(datestr) == 0 or not datestr.isdigit():
        logger.error(u"入参错误: 账期不正确[%s]!", datestr)
        sys.exit()
    if step == "all":
        logger.error(u"入参错误: 必须指定对账步骤")
        sys.exit()
    logger.debug(u"入参: 账期[%s], 类型[%s], 省份[%s]!", datestr, type, prov)
    if step == "1" or step == "all":
        logger.debug(u"开始对账第一步ftp下载文件")
        sftpTools.get_account_file(datestr, type, prov)
    if step == "2" or step == "all":
        logger.debug(u"开始导出卡券中心原始数据")
        dataExport.getAccountData(datestr, type, prov)
    if step == "3" or step == "all":
        logger.debug(u"开始第一次比对")
        fileCmp.cmpallfist(datestr, type, prov)
    if step == "4" or step == "all":
        logger.debug(u"开始补充数据")
        mysqlopt.procbusiOnlyfile(datestr, type, prov)
    if step == "5" or step == "all":
        logger.debug(u"开始第二次比对")
        fileCmp.cmpallsecond(datestr, type, prov)
    if step == "7" or step == "all":
        logger.debug(u"开始处理差异文件")
        procresult.procalldiff(datestr, type, prov)

