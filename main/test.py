#!/tongweb/bin/python/python2.7/bin/python
#coding:utf8




import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper,createPath
from conf import config
import platform
yesterday=getDate.getYesterday()


#当前时间20180126-183918
loggerDate = getDate.getToday()


log_dir=createPath.mkdir(config.log_dir)
logger = printLog.printLog(loggerDate, "version", log_dir)

dataBase = config.ecora
def getPhoneFile():
        conn_data = dataBase


        try :
            print '开始连接数据库'
            cur, conn = dbOper.conn_db(conn_data)  # 获取数据库连接cur
            print '连接数据库成功'
            logger.info('连接数据库成功')
            print platform.python_version()

        except Exception  as e:
            print '连接数据库失败：'
            print  e
            print platform.python_version()
            logger.info('连接数据库失败')
            logger.info(e)








if __name__ == "__main__":
    for a in ('15032609451','asd','assssssssss','','还送货的'):
        if a.__len__()==11:
            print a