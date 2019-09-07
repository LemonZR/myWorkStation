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


    def query(self,phones ):
        result =[]


        for phone in phones:
            historyResultFile = '%s/jcy/result/%s.csv' % (BASE_DIR, phone)
            historyResult = open(historyResultFile, "a")  # 记录查询历史的文件history，也可以用来存输出结果。
            sql = """select l.VALID_FLAG, l.LOGIN_NAME as 姓名, 
            l.LOGIN_NO as 账号编号, l.CONTACT_PHONE 电话,l.UPDATE_TIME,l.EMAIL ,ar.AREA_NAME as 所属机构,sh.SHOP_NAME 商户, r.ROLE_NAME as 角色,l.CREATE_TIME 账号创建时间 
            FROM td_padm_login_info l 
            LEFT JOIN td_padm_login_role_rel lr ON l.LOGIN_ID= lr.LOGIN_ID
            LEFT JOIN td_padm_role r on lr.ROLE_ID=r.ROLE_ID
            LEFT JOIN td_padm_login_shop s ON l.LOGIN_ID= s.LOGIN_ID
            LEFT JOIN td_padm_shop sh on s.SHOP_ID =sh.SHOP_ID
            LEFT JOIN td_padm_area ar ON l.AREA_IDS =ar.AREA_CODE
            where 
            #l.CONTACT_PHONE='%s'
                    """%(phone)
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
        phone = str(sys.argv[1]).strip()
        print "phone is: "+ phone
        phones =(phone,)
        run.query(phones)

    except Exception as e:
        logger.error("脚本无参数执行，读取table文件查询")
        phone_no ='phone_no.conf'