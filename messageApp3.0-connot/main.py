#!/e3base/python/Python-2.7.6/python
# coding:utf8

import os
import sys
import time, datetime
from common import hadoop
from common.myException import myException
import threading
import multiprocessing

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from common.strToDate import myDate
import Queue
from common.dateCalculator import dayAdd, monthAdd

myDate = myDate()
today = time.strftime('%Y%m%d', time.localtime(time.time()))
yesterday = (datetime.datetime.today() + datetime.timedelta(-1)).strftime('%Y%m%d')
import logging

logFile = os.sep.join((BASE_DIR, "messageApp3.0", "log", today + ".log"))
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
    # 判断结果
    if not isPathExists:
        # 如果不存在则创建目录
        os.makedirs(path)
        log.info(path + ' 创建成功')
        return path
    else:
        return path


def getBatchDict(batchFile):
    with open('%s' % (batchFile), 'r') as batchInfos:
        batchDict = {}

        for line in batchInfos:
            batchLine = line.split("|")
            batch = batchLine[0].strip()
            batchDict[batch] = {}
            try:
                # obtainBeginTime like "2019-08-01"
                batchDict[batch]["obtainBeginTime"] = batchLine[1].strip().split('.')[0]
                batchDict[batch]["obtainEndTime"] = batchLine[2].strip().split('.')[0]
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
            obtainBeginTime = info["obtainBeginTime"]
            obtainEndTime = info["obtainEndTime"]
            if not termType.isdigit():
                log.warn(batch + "'s termType is not a number")
                continue

            if termType == '1':

                expireTimestr = info["expireTime"]
                expireTime = myDate.toDate("%Y%m%d", expireTimestr)
                expireDay = info["expireDay"]
                if dayAdd(scanDate, expireDay) == expireTime:
                    filteredDict[batch] = batchDict[batch]
                else:
                    pass


            elif termType == '2':

                # expireTime is like '7'
                expireTime = info["expireTime"]
                obtainBeginDate = myDate.toDate("%Y%m%d", obtainBeginTime)
                obtainEndDate = myDate.toDate("%Y%m%d", obtainEndTime)
                expireDay = info["expireDay"]
                if dayAdd(obtainBeginDate, expireTime) <= dayAdd(scanDate, expireDay) <= dayAdd(obtainEndDate,
                                                                                                expireTime):
                    filteredDict[batch] = batchDict[batch]
                else:
                    pass


            elif termType == '3':
                obtainBeginTime = info["obtainBeginTime"]
                obtainEndTime = info["obtainEndTime"]
                # expireTime is like '7'
                expireTime = info["expireTime"]
                theLastDayOBT = dayAdd(monthAdd(myDate.toDate("%Y%m01", obtainBeginTime), 1), -1)
                theLastDayOET = dayAdd(monthAdd(myDate.toDate("%Y%m01", obtainEndTime), 1), -1)
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
    timeStruct = time.strptime(qryDate, "%Y%m%d")
    yearstr = time.strftime("%Y", timeStruct)
    monthstr = time.strftime("%m", timeStruct)
    daystr = time.strftime("%d", timeStruct)
    batch_id = '$1'
    term_type = '$3'
    expire_warn = '$51'
    batch_status = '$6'
    obtain_begin_time = '$10'
    obtain_end_time = '$9'
    expire_time = '$5'
    expire_remind_desc = '$32'
    expire_day = '$33'
    expire_temp_id = '$36'
    fsPath = hadoop.path('batch', yearstr, monthstr, daystr)
    hadoopCmd = "hadoop fs -cat %s |awk -F '$' '{if(%s==0&&%s==3)print %s\"|\"%s\"|\"%s\"|\"%s\"|\"%s\"|\"%s\"|\"%s\"|\"%s}'" \
                % (
                    fsPath, expire_warn, batch_status, batch_id, obtain_begin_time, obtain_end_time, term_type,
                    expire_time,
                    expire_day, expire_remind_desc, expire_temp_id)
    log.info("%s" % hadoopCmd)
    try:
        a = hadoop.hadoopToFile(hadoopCmd, batchFile)
        if a == 0:

            log.info('Generate batch data success')
        else:
            log.error('Generate batch data failed')
            exit(1)
    except Exception as e:
        raise myException(e, 'Generate batch data failed')


def generatePhoneDataCmd(filteredDict):
    q = Queue.Queue()
    for batch, info in filteredDict.items():
        try:
            expireDay = info['expireDay']
            expireTime = myDate.toDate('%Y-%m-%d', dayAdd(today, expireDay))

            obtainBeginTime = info["obtainBeginTime"]
            obtainBeginDate = myDate.toDate("%Y%m%d", obtainBeginTime)
            expire_time = '$8'
            lifecycle_st = '$12'
            bind_no = '$19'
            tmp = obtainBeginDate
            a=myDate.yesMon
            b=myDate.toYm(tmp)
            while myDate.toYm(tmp) <= myDate.toYm(yesterday):
                year = myDate.toDate("%Y", tmp)
                month = myDate.toDate("%m", tmp)
                fsPath = hadoop.path('card', '%s', '%s', '*') % (year, month, )
                cmd = """hadoop fs -cat %s |grep %s |awk -F '|' '{if(substr(%s,0,10)=="%s"&&%s=="8")print %s"|"%s}'""" % (
                    fsPath, batch, expire_time, expireTime, lifecycle_st, batch, bind_no)
                q.put(cmd)
                # log.info(cmd)
                # log.info("%s" % fsPath)
                tmp = monthAdd(tmp, 1)
                # log.info("%s----%s--%s-----%s"%(tmp,obtainBeginDate,obtainEndDate,yesterday))
        except Exception as e:
            log.error(e)
    log.info("[hadoop cmd's Queue count: %s]" % q.qsize())
    return q


def mutiTWork(cmdQueue, filePath, tcount=30):
    threads = []
    for i in range(tcount):
        t = threading.Thread(target=hadoop.hadoopToFileQ, args=(cmdQueue, filePath))
        threads.append(t)
    for thread in threads:
        thread.setDaemon(True)
        thread.start()
    for thread in threads:
        thread.join()

def mutiPWork(cmdQueue, filePath, pcount=30):
    p = multiprocessing.Pool()
    list_count=int(cmdQueue.qsize()/pcount)+1
    cmdDict={}
    for i in range(pcount):
        cmdDict[i]=[]
        for j in range(list_count):
            try:
                cmdDict[i].append(cmdQueue.get(block=False))
            except  :
                if cmdQueue.qsize()==0:
                    log.info("hadoop command Queue is empty now")
                else:
                    log.warn("[Unkown] hadoop command Queue is %s now"%cmdQueue.qsize())
                break
    print cmdDict.__len__()
    print list_count,cmdDict[29].__len__()
    for i in range(pcount):
        cmdlist=cmdDict[i]
        p.apply_async(func=hadoop.hadoopToFileL ,args=(cmdlist,filePath))

    p.close()
    p.join()

def generateFinalData(filteredBatchDict, phoneFile, resultFile):
    result = open(resultFile, 'a')
    with open('%s' % (phoneFile), 'r') as batchPhone:
        numberA = 0
        for line in batchPhone:
            if numberA == 9999:
                numberA = 0
            numberA += 1
            list = line.split("|")
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


def scpFileToMsgCt(finalDataFile):
    try:

        log.info("scp local file to message center start......")
        # [step 5]
        # messageCenter = msgCt
        msgCtHost = '10.255.201.220'
        msgCtUser = 'echnlog'
        msgCtPwd = '''g3+{C'0SW,7kd>tA6*}[X"~]a'''
        msgCtDir = '/echnlog/couponsdata'
        scpSh = os.sep.join((BASE_DIR, "messageApp3.0", "scp", "scp.sh"))
        localFile = finalDataFile

        log.info('%s %s %s %s  %s' % (scpSh, msgCtHost, msgCtUser, localFile, msgCtDir))
        # [step 6]  scp file to message center
        os.system('%s %s %s  %s %s' % (scpSh, msgCtHost, msgCtUser, localFile, msgCtDir))
        log.info("scp local file to message center succeed")
    except Exception as e:
        log.error(e)
        log.error("scp local file to message center failed")


if __name__ == "__main__":

    qryDate = yesterday
    fileDir = os.sep.join((BASE_DIR, "messageApp3.0", "messageFile", today))
    mkdir(fileDir)
    batchFile = os.sep.join((fileDir, "batchFile-%s.txt" % today))
    filteredBatchFile = os.sep.join((fileDir, "filteredBatchFile-%s.txt" % today))
    phoneFile = os.sep.join((fileDir, "phoneFile-%s.txt" % today))
    timexx = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    finalDataFile = os.sep.join((BASE_DIR, "messageApp3.0", "messageFile", today, "2_%s01.txt" % timexx))

    generateBatchData(batchFile, qryDate)
    # 2
    batchDict = getBatchDict(batchFile)
    log.info("Total batch number is %s " + batchDict.__len__().__str__())

    filteredBatchDict = batchInfoFilter(batchDict, filteredBatchFile)
    log.info("The number after filtering is " + filteredBatchDict.__len__().__str__())
    # 3-----------------------------important---------------
    cmdQueue = generatePhoneDataCmd(filteredBatchDict)
    #[mutiPWork] for multiprocessing  ,[mutiTWork] for mutithreading
    mutiPWork(cmdQueue, phoneFile)
    log.info("Multitasking finished ")
    try:
        generateFinalData(filteredBatchDict, phoneFile, finalDataFile)
        log.info("Generate final data file success")
    except Exception as e:
        log.error(e)
    try:
        pass
        #scpFileToMsgCt(finalDataFile)
    except Exception as e:
        log.error(e)
