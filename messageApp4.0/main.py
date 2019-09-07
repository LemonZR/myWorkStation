#!/e3base/python/Python-2.7.6/python
# coding:utf8

import os
import sys
import time, datetime
from dateutil.relativedelta import relativedelta
from common.dateCalculator import dayAdd, monthAdd
from common.myException import myException
from common.strToDate import myDate

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
myDate = myDate()
today = time.strftime('%Y%m%d', time.localtime(time.time()))
yesterday = (datetime.datetime.today() + datetime.timedelta(-1)).strftime('%Y%m%d')
import logging

logFile = os.sep.join((BASE_DIR, "messageApp4.0", "log", today + ".log"))
formatter = logging.Formatter('%(asctime)s - %(levelname)s[line:%(lineno)d]: %(message)s')
fh = logging.FileHandler(logFile)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
log = logging.getLogger()
log.addHandler(fh)
log.addHandler(ch)
log.setLevel(logging.DEBUG)


def mkdir(path):
    path = path.strip()
    path = path.rstrip("\\")
    isPathExists = os.path.exists(path)
    # u'判断结果'
    if not isPathExists:
        # 如果不存在则创建目录
        os.makedirs(path)
        log.info(path + u' 创建成功')
        return path
    else:
        return path


def hiveToFile(sql, filePath, pid=-1):
    starttime = datetime.datetime.now()
    command = 'hive -e "%s" >> %s' % (sql, filePath)

    log.info('%s [process %s started]' % (sql, pid))
    a=0
    #a = os.system(command)
    endtime = datetime.datetime.now()

    if a == 0:
        log.info('[process %s success ,Time:%ss]' % (pid, (endtime - starttime).seconds))
    else:
        log.info('[process %s  failed ,Time:%ss]' % (pid, (endtime - starttime).seconds))
    return a


def getBatchDict(batchFile):
    with open('%s' % (batchFile), 'r') as batchInfos:
        batchDict = {}

        for line in batchInfos:
            batchLine = line.split("\t")
            batch = batchLine[0].strip()
            batchDict[batch] = {}
            try:
                # obtainBeginTime like "2019-08-01"
                batchDict[batch]["obtainBeginTime"] = batchLine[1].strip()
                batchDict[batch]["obtainEndTime"] = batchLine[2].strip()
                batchDict[batch]["termType"] = batchLine[3].strip()
                batchDict[batch]["expireTime"] = batchLine[4].strip()
                batchDict[batch]["expireDay"] = batchLine[5].strip()
                batchDict[batch]["msg"] = batchLine[6].strip('"')
                batchDict[batch]["expireTempId"] = batchLine[7].strip()
            except Exception as e:
                log.warn(e.__str__() + '\n[getBatchDict(batchFile):' + batch)
                # del then key
                batchDict.pop(batch)
                continue

        return batchDict


def batchInfoFilter(batchDict, newBatchFile, scanDate=today):
    filteredDict = {}
    for batch, info in batchDict.items():
        try:
            termType = info["termType"]

            if not termType.isdigit():
                log.warn(batch + "'s termType is not a number")
                continue

            if termType == '1':
                expireTime = myDate.toDate("%Y-%m-%d", info["expireTime"])
                expireDay = info["expireDay"]
                if dayAdd(scanDate, expireDay) == expireTime:
                    filteredDict[batch] = batchDict[batch]
                else:
                    pass


            elif termType == '2':

                obtainBeginDate = info["obtainBeginTime"]
                # expireTime is like '7'
                expireTime = info["expireTime"]
                obtainEndDate = info["obtainEndTime"]
                expireDay = info["expireDay"]
                if dayAdd(obtainBeginDate, expireTime) <= dayAdd(scanDate, expireDay) <= dayAdd(obtainEndDate,
                                                                                                expireTime):
                    filteredDict[batch] = batchDict[batch]
                else:
                    pass


            elif termType == '3':
                obtainBeginDate = info["obtainBeginTime"]
                # expireTime is like '7'
                expireTime = info["expireTime"]
                obtainEndDate = info["obtainEndTime"]

                theLastDayOBT = dayAdd(monthAdd(myDate.toDate("%Y%m01", obtainBeginDate), 1), -1)
                theLastDayOET = dayAdd(monthAdd(myDate.toDate("%Y%m01", obtainEndDate), 1), -1)
                expireDay = info["expireDay"]
                if dayAdd(theLastDayOBT, expireTime) <= dayAdd(scanDate, expireDay) <= dayAdd(theLastDayOET,
                                                                                              expireTime):
                    filteredDict[batch] = batchDict[batch]
                else:
                    pass

            else:
                log.error("[batchInfoFilter]termType is wrong:[termType: %s ]" % termType)
        except Exception as e:
            log.error(e.__str__() + " wrong")
            continue
    with open(newBatchFile, 'a') as newfile:
        for batch, info in filteredDict.items():
            newfile.write(batch + "|" + "|".join(info.values()) + "\n")
    return filteredDict


def generateBatchData(batchFile, qryDate):
    # 先转换为时间数组,然后转换为其他格式qryDate = '20190801'
    timeStruct = time.strptime(qryDate, "%Y%m%d")
    yearstr = time.strftime("%Y", timeStruct)
    monthstr = time.strftime("%m", timeStruct)
    daystr = time.strftime("%d", timeStruct)

    sql = "select batch_id,to_date(obtain_begin_time),to_date(obtain_end_time),term_type,expire_time,expire_day,expire_remind_desc,expire_temp_id " \
          "from pods.coupons_v2_td_def_pcard_batch " \
          "where expire_warn='0' and batch_status='3'and yearstr='%s' and monthstr='%s'and daystr='%s'" % (
              yearstr, monthstr, daystr)
    hiveToFile(sql, batchFile)


def genPhoDatSqlList(filteredDict):
    sqlList = []
    for batch, info in filteredDict.items():
        expireDay = info['expireDay']
        expireTime = info['expireTime']
        obtainBeginTime = info['obtainBeginTime']
        # obtainBeginTime = time.strptime(info["obtainBeginTime"], "%Y-%m-%d")
        termType = info["termType"]

        if termType == '1':
            card_expire_time = myDate.toDate('%Y-%m-%d', expireTime)

            obd = myDate.toDate("%d", obtainBeginTime)
            obm = myDate.toDate("%m", obtainBeginTime)
            oby = myDate.toDate("%Y", obtainBeginTime)
            sql = "select batch_id,bind_no from pods.coupons_v2_td_pcard_info  " \
                  "where batch_id ='%s' and to_date(expire_time)='%s' and lifecycle_st ='8' " \
                  "and yearstr >='%s' and monthstr >='%s' and daystr >='%s'" % (batch, card_expire_time, oby, obm, obd)
            sqlList.append(sql)
        elif termType == '2':
            card_expire_time = myDate.toDate('%Y-%m-%d', dayAdd(today, expireDay))
            card_bind_time = dayAdd(card_expire_time, -(int(expireTime)))
            oby = myDate.toDate("%Y", card_bind_time)
            obm = myDate.toDate("%m", card_bind_time)
            obd = myDate.toDate("%d", card_bind_time)
            sql = "select batch_id,bind_no from pods.coupons_v2_td_pcard_info  " \
                  "where batch_id ='%s' and to_date(expire_time)='%s' and lifecycle_st ='8' " \
                  "and yearstr ='%s' and monthstr ='%s' and daystr ='%s'" % (batch, card_expire_time, oby, obm, obd)

            sqlList.append(sql)
        elif termType == '3':
            card_expire_time = myDate.toDate('%Y-%m-%d', dayAdd(today, expireDay))
            card_bind_time = monthAdd(card_expire_time, -(int(expireTime)))
            oby = myDate.toDate("%Y", card_bind_time)
            obm = myDate.toDate("%m", card_bind_time)
            # obd= myDate.toDate("%d", card_bind_time)
            sql = "select batch_id,bind_no from pods.coupons_v2_td_pcard_info  " \
                  "where batch_id ='%s' and to_date(expire_time)='%s' and lifecycle_st ='8' " \
                  "and yearstr ='%s' and monthstr ='%s' " % (batch, card_expire_time, oby, obm)
            sqlList.append(sql)
        else:
            log.info("[genPhoDatSqlList]termType Error:" + termType)
            continue

    return sqlList


def mutiPWork(sqlList, filePath):
    import multiprocessing
    p = multiprocessing.Pool()

    for i in range(sqlList.__len__()):
        sql = sqlLiist[i]
        p.apply_async(func=hiveToFile, args=(sql, filePath, i))
    p.close()
    p.join()

def phoneDataFilter(phoneFile):
    with open('%s' % (phoneFile), 'r') as phone_data:
        for lines in phone_data:
            list=lines.split()
            batch = list[0]
            phone = list[1]
            passwd = list[2]
            status = list[3]
            time = list[4]
            continue
            
def generateFinalData(filteredBatchDict, phoneFile, resultFile):
    numberA = 0
    numberB = 0
    with open('%s' % (phoneFile), 'r') as batchPhone:
        result_file_name='%s%02d.txt'%(resultFile,numberB)
        result = open(result_file_name, 'a')
        for line in batchPhone:
            if numberA == 9999:
                time.sleep(1)
                numberA = 0
                numberB += 1
                if numberB >99:
                    break
                result_file_name = '%s%02d.txt' % (resultFile, numberB)
                result = open(result_file_name, 'a')
            numberA += 1
            list = line.split()
            batch = list[0].strip()
            phone = list[1].strip('\n')
            msg = filteredBatchDict[batch]["msg"]
            expireTempId = filteredBatchDict[batch]["expireTempId"]
            timex = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
            lineA = "1101%s%04d" % (timex, numberA)
            lineB = expireTempId
            lineC = '{"msg":"%s"}' % msg
            lineD = phone
            lineE = "pcard"
            lineF = timex
            result.write('%s|%s|%s||%s|%s||||%s||||||\n' % (lineA, lineB, lineC, lineD, lineE, lineF))


if __name__ == "__main__":

    qryDate = yesterday
    # fileDirTime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    fileDirTime = today
    fileDir = os.sep.join((BASE_DIR, "messageApp4.0", "messageFile", fileDirTime))
    mkdir(fileDir)

    batchFile = os.sep.join((fileDir, "batchFile-%s.txt" % today))
    filteredBatchFile = os.sep.join((fileDir, "filteredBatchFile-%s.txt" % today))
    phoneFile = os.sep.join((fileDir, "phoneFile-%s.txt" % today))

    timexx = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    finalDataFile = os.sep.join((BASE_DIR, "messageApp4.0", "messageFile", today, "2_%s" % timexx))
    try:
        # 1
        #generateBatchData(batchFile, qryDate)
        # 2
        batchDict = getBatchDict(batchFile)
        log.info("Total batch number is %s " + batchDict.__len__().__str__())
        filteredBatchDict = batchInfoFilter(batchDict, filteredBatchFile)
        log.info("The number after filtering is " + filteredBatchDict.__len__().__str__())
        # 3
        sqlLiist = genPhoDatSqlList(filteredBatchDict)
        mutiPWork(sqlLiist, phoneFile)
        log.info("Generate PhoneData success")
        # 4
        generateFinalData(filteredBatchDict, phoneFile, finalDataFile)
        log.info("Generate final data file success")
    except Exception as e:
        log.error(e)

    try:

        log.info("scp local file to message center start......")
        # 5
        # messageCenter = msgCt
        msgCtHost = '10.255.201.220'
        msgCtUser = 'echnmarket'
        msgCtPwd = '''g3+{C'0SW,7kd>tA6*}[X"~]a'''
        msgCtDir = '/echnlog/couponsdata'
        scpSh = os.sep.join((BASE_DIR, "messageApp4.0", "scp", "scp.sh"))
        localFile = finalDataFile

        log.info('%s %s %s %s  %s' % (scpSh, msgCtHost, msgCtUser, localFile, msgCtDir))
        # os.system('%s %s %s  %s %s' % (scpSh,msgCtHost, msgCtUser, localFile, msgCtDir))
        log.info("scp local file to message center succeed")
    except Exception as e:
        log.error(e)
        log.error("scp local file to message center failed")
