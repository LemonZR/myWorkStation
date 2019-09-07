#coding:utf8


import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper,createPath
from conf import config
import sendcard
import sendEmail

today = getDate.getToday()

dataBase = config.ecora
# 当前时间20180126-183918
loggerDate = getDate.getToday()


# yesterday='20180125'
class runMe(object):
    def __init__(self, pcard_name=None, logger=None):
        self.pcardName = pcard_name
        self.resData = createPath.mkdir(config.data_dir +'/' + pcard_name)
        self.logger=printLog.getLogger(loggerDate, pcard_name, createPath.mkdir(config.log_dir))
        self.phoneFileName = pcard_name + today

    def getOptSeq(self):
        #返回券列表
        # return ["1020171215941493862723223552_1320171215941493862966886400_991"]
        #返回券
        actName = self.pcardName
        if actName == 'qx20':
            return "1020171215941493862723223552_1320171215941493862966886400_991"
        elif actName == 'xj35':
            return "1020171215941494680037883904_1320171215941494680122163200_991"
        elif actName == 'xj75':
            return "1020171215941495201666695168_1320171215941495201943912448_991"
        else:
            return  None

    def getSql(self):
        pcard_name = self.pcardName
        if actName == 'xj15':
            chargefee = '5000'
        elif actName == 'xj35':
            chargefee = '10000'
        elif actName == 'xj75':
            chargefee = '20000,30000,400000,50000'
        else:
            return None

        sql = """select phone_no, pay_time  from tw_ptl_payprocess_log%s   where 
          rate_operate_id='1778' and prov_code='991' 
        and order_type='0'
        and  charge_fee  in (%s)
        and  order_status in ('2','4','5')
        """%(today,chargefee)
        return sql

    def getPhoneFile(self):
        phone_file ='%s/%s.dat' % (self.resData, self.phoneFileName)
        logger = self.logger
        if os.path.exists(r'%s'%(phone_file)):
            print '号码文件已经生成，请注意是否需要重新生成'
            print "号码文件:%s" % phone_file
            logger.info("号码文件已经生成，请注意是否需要重新生成")
            print "sql is:" +self.getSql()
        else:
            conn_data = dataBase
            optSeq =self.getOptSeq()
            # print conn_data
            # 调用数据库连接模块，生成数据库连接cur
            print '开始连接数据库'
            logger.info("开始连接数据库")
            try:
                cur, conn = dbOper.conn_db(conn_data)  # 获取数据库连接cur
                print '连接数据库成功：'
                print cur ,conn
                logger.info("连接数据库成功")
            except Exception as e:
                print("连接数据库失败")
                print e
                logger.info("连接数据库失败")
                logger.info(e)


            try:
                sql = self.getSql()
                logger.info("sql is" + sql)
                print 'getSql()\'s result :' + sql
                print "查询开始" + sql, self.pcardName
                head,result = dbOper.qryData(cur,sql)
                print "查询完成"
                # 写号码文件
                print '开始生成号码文件'
                with open(phone_file, "a") as phoneFile :
                    for res in result:
                        print (res)
                        print (type(res))
                        phoneFile.write("%s,%s,%s"%(res[0],res[1],optSeq))
                        phoneFile.write("\n")
                print "号码文件生成结束:%s" % phone_file
                phoneFile.close()
            except Exception as e:
                print("查询数据失败")

        return  '%s/%s' % (self.resData, self.phoneFileName)
    #发送优惠券
    def run(self):
        actName = self.pcardName
        optSeq = self.getOptSeq()
        #生成号码文件，并返回文件名
        phoneFile= self.getPhoneFile()
        print ("发券开始")
        logger = self.logger
        logger.info("发券开始sendcardstart" )
        if os.path.exists(phoneFile+'.dat'):
            if os.path.getsize(phoneFile+'.dat'):
                print os.path.getsize(phoneFile+'.dat')
                try:
                    sendcard.run(logger, phoneFile, optSeq)
                    logger.info("发券完成 success")
                    print ("发券完成")
                except Exception as e:

                    print e
                    print ("发券失败 failed")
                    logger.info(e)
                    logger.info("发券失败 failed")
            else:
                print '号码文件为空,本次程序终止！'
                return
        else:
            print  '号码文件不存在,本次程序终止！'











if __name__ == "__main__":
    for actName in ['xj15', 'xj35', 'xj75']:

        rr = runMe(actName)
        print rr.pcardName
        print rr.getOptSeq()
        rr.run()
        print rr.resData,rr.phoneFileName
        try:
            sendEmail.sendEmail(rr.logger, rr.resData, rr.pcardName)
        except Exception as e:
            print e
            print "邮件发送失败"
            rr.logger.info("邮件发送失败")