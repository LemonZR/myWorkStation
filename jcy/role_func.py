#coding:utf8




import os
import time
import sys
#########
#python2 需要对编码进行处理
reload(sys)
sys.setdefaultencoding('utf8')
###########
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import  dbOper,printLog,getDate
from conf import  config
loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"jcy","%s/jcy/log/"%BASE_DIR)

def getConnect():
    try:
        cur,conn = dbOper.conn_db_new(config.jcy)
        logger.info("连接数据库成功")
    except Exception as e:
        logger.error("连接数据库失败")
        cur,conn = None,None
    return  cur,conn
class jcy(object):
    def __init__(self):
        #self.count =0
        self.cur,self.conn =getConnect()
        self.logger = logger
        if self.cur ==None:
            logger.error("初始化数据库连接失败，退出")
            sys.exit(1)


    def query(self,roles ):
        result =[]


        for role in roles:
            historyResultFile = '%s/jcy/result/%s.csv' % (BASE_DIR, role)
            historyResult = open(historyResultFile, "a")  # 记录查询历史的文件history，也可以用来存输出结果。
            sql = """select
            r.ROLE_NAME 角色 ,b1.NAVI_NAME 一级菜单,b2.NAVI_NAME 二级菜单,b3.NAVI_NAME 三级菜单 ,ai.APP_NAME,a.APP_VERSION_URL,a.VERSION_CREATE_DATE 创建时间
            from td_padm_role r
            
            LEFT JOIN  td_padm_role_function_rel ra ON r.ROLE_ID= ra.ROLE_ID
            LEFT JOIN  td_padm_app_version a ON ( ra.APP_ID =a.APP_ID and ra.APP_VERSION= a.APP_VERSION )
            LEFT JOIN  td_padm_app_channel_rel an ON ( a.APP_ID = an.APP_ID and a.APP_VERSION = an.APP_VERSION)
            
            LEFT JOIN td_padm_portal_nav b3 ON an.NAVI_IDS =b3.NAVI_ID
            LEFT JOIN td_padm_portal_nav b2 ON b3.PARENT_NAVI_ID=b2.NAVI_ID
            LEFT JOIN td_padm_portal_nav b1 ON b2.PARENT_NAVI_ID=b1.NAVI_ID
            LEFT JOIN td_padm_app_info ai ON a.APP_ID =ai.APP_ID
            where #a.APP_VERSION_TYPE ='1' and 
            r.ROLE_NAME like '%%'
            ORDER BY r.ROLE_ID , b1.NAVI_PRIORITY DESC,  b1.NAVI_ID , b2.NAVI_PRIORITY DESC,  b2.NAVI_ID , b3.NAVI_PRIORITY DESC,  b3.NAVI_ID ,ai.APP_NAME
                    """%(role)
            logger.info(sql)

            try:
                qryHead,qryResult = dbOper.qryData(self.cur, sql)

            except Exception as e:
                self.logger.error(e)
                self.logger.error ("查询失败")
            resultbody = []
            resulthead = []
            for j in range(0, len(qryHead)):
                resulthead.append(str(qryHead[j]))
            result.append(','.join(resulthead))  # 首先写入头信息
            historyResult.write(','.join(resulthead) + "\n")
            print (','.join(resulthead) + "\n")
            count = len(qryResult)

            for i in range(0, qryResult.__len__()):
                # 查询结果
                resultbody.append([])  # 长度+1
                for ii in range(0, len(qryResult[i])):
                    resultbody[i].append("'" + str(qryResult[i][ii]) + "'")
            for i in range(0,len(resultbody)):
                result.append(','.join(resultbody[i]))
                historyResult.write(','.join(resultbody[i])+"\n")
                print (','.join(resultbody[i])+"\n")

        time.sleep(0.05)
        self.logger.error("Query finished")
        logger.error("计数：" + str(count))
        historyResult.close()
        return result

    def batchQuery(self,tablesinfo):
        self.logger.error("Query start")
        with open(tablesinfo,'r') as login_no_list:
            for login_no in login_no_list:
                self.query(login_no.strip())



if __name__== "__main__":

    run = jcy()
    try:
        role = str(sys.argv[1]).strip()
        print "role is: "+ role
        roles =(role,)
        run.query(roles)

    except Exception as e:
        logger.error("脚本无参数执行，读取table文件查询")
        roles ='roles.conf'