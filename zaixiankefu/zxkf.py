#!/tongweb/bin/python/python2.7/bin/python
# coding:utf8

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from Utils import dbOper,getDate
import json
import requests
from conf import config
#from urllib import quote //urlEncode 用
#import string //urlEncode 用
sys.dont_write_bytecode = True

sql = "SELECT SUBSTRING(COMMENTS.CREATE_TIME,1,8) AS COMMENT_DATE, COMMENTS.TENANT_ID AS COMMENT_TENANT_ID, COMMENTS.COMMODITY_ID AS COMMENT_COMMODITY_ID, " \
      "COMMENTS.CHANNEL AS COMMENT_CHANNEL, COMMENTS.COMMODITY_NAME AS COMMENT_COMMODITY_NAME, tag.LABEL_ID AS COMMENT_LABEL_ID," \
      " count(1) AS COMMENT_REPORT_COUNT FROM td_cmt_comment AS COMMENTS LEFT JOIN td_cmt_commnet_tag_rel tag ON COMMENTS.COMMENT_ID = tag.COMMENT_ID" \
      " WHERE SUBSTRING(COMMENTS.CREATE_TIME,1,8) >= ? AND SUBSTRING(COMMENTS.CREATE_TIME,1,8) <= ? " \
      "GROUP BY COMMENT_DATE, TENANT_ID, COMMENT_CHANNEL, COMMENT_COMMODITY_NAME, tag.LABEL_ID " \
      "ORDER BY COMMENT_DATE DESC, TENANT_ID, COMMENT_CHANNEL, COMMENT_COMMODITY_NAME, tag.LABEL_ID, COMMENT_REPORT_COUNT DESC limit ?,?"


def getCur():
    conn_data = config.jcy_local
    print '初始化数据库连接'
    try:
        cur, conn = dbOper.conn_db_new(conn_data)  # 获取数据库连接cur
        print '连接数据库成功'
        return cur,conn
    except Exception as e:
        print("连接数据库失败")
        sys.exit(0)

class runMe(object):
    def __init__(self):
        self.cur,self.conn= getCur()
        #print(self.getSql("phone"))
    def getSql(self,phone):

        sql = """SELECT DISTINCT l.LOGIN_ID,l.MISASSPER_ID,l.CONTACT_PHONE, ls.SHOP_ID,
                      CASE r.ROLE_NAME
                      WHEN "在线客服-客服" THEN -1
                      ELSE 2
                      END as flag,
                      r.ROLE_NAME
                      from basedb.td_padm_login_info l
                      LEFT JOIN  basedb.td_padm_login_shop ls ON  l.LOGIN_ID=ls.LOGIN_ID
                      LEFT JOIN basedb.td_padm_login_role_rel lr ON l.LOGIN_ID=lr.LOGIN_ID
                      LEFT JOIN basedb.td_padm_role r ON r.ROLE_ID= lr.ROLE_ID
                      WHERE l.CONTACT_PHONE = '%s'""" % (phone) + ' AND r.ROLE_NAME like "%在线客服%" '
        return sql

    #查询基础域工号表信息
    #l.CONTACT_PHONE = '%s'""" % (phone)+' AND
    def getInfo(self,phone):
        cur=self.cur
        sql = self.getSql(phone)
        print "查询开始"
        head,result = dbOper.qryData(cur,sql)
        print "查询完成"
        #exception = []
        if result.__len__()>0:

            flag = 1
            login_id = result[0][0]

            #判断账号是否同时含有客服主管和客服角色
            for res in result:
                #判断同一个手机号是否存在多个工号
                if login_id !=res[0]:

                    print("该号码" + phone.__str__() + "存在不一致工号" + str(login_id) + "和" + str(res[0]))
                    #exception .append("该号码" + phone.__str__() + "存在不一致工号" + str(login_id) + "和" + str(res[0]))
                    return
                try:
                    flag *= res[-2]
                except Exception as e:
                    flag = 0
                roleName = res[-1]
                shopid  = res[3]
                print("login_id:%s  role:%s  shop_id:%s") % (login_id, roleName,shopid)
            print "flag is ："+flag.__str__()

            if flag == -1:
                agent_type = '0'
                print("该账号只含客服角色")
            elif flag <-1:
                agent_type ='0,1'
                print("该账号同时包含客服和其他角色")
            elif flag >1:
                agent_type ='1'
                print("该账号不含客服角色")
            else:
                print("我不知道这是咋地了")
                return
                #return
            #print("没有在线客服角色我还执行了")
            agentType = agent_type
            print "agentType will be "+agentType
            loginId =login_id.encode('utf8') #result[0][0]
            #misassperId = quote((result[0][1]).encode('utf8'),safe=string.printable),params 如果是字符串拼接的用这个
            misassperId = (result[0][1]).encode('utf8')
            phone =  (result[0][2]).encode('utf8')
            shop_id = (result[0][3])
            if shop_id == None:
                pass
            else:
                shop_id =(result[0][3]).encode('utf8')

            finalInfo ={
                'agentType':agentType,
                'loginId' :loginId,
                'misassperId':misassperId,
                'phone':phone,
                'shopId':shop_id
            }
            return finalInfo
        else:
            print("该号码在基础域工号表不存在在线客服类角色："+phone.__str__())
            return


    def toRequest(self,phone):

        info =self.getInfo(phone)
        req = requests.session()

        if info !=None:
            id =info["loginId"]
            alias =info["misassperId"]
            phoneNumber =info["phone"]
            agentType =info["agentType"]
            tenantId =info ["shopId"]
            modifiedTime =getDate.getToday()
            print (str(phone)+"-------------------request start")
            header = {'content-type': 'application/json',
                      'cookie': 'test=lalala'}
            url = "http://221.176.60.135:80/i/livechat-web-console/rest/agentSynchronousController/insertOrUpdateAgent" #这是公网ip访问
            #url ="http://bd.shop.10086.cn/i/livechat-web-console/rest/agentSynchronousController/insertOrUpdateAgent?id=cecef80c12f54284b1a59be67df3c024&phoneNumber=13932176881&tenantId=1000047&agentType=1"
            if tenantId ==None or tenantId=='-------':
                params={"id":id,"alias":alias,"phoneNumber":phoneNumber,"tenantId":1,"agentType":agentType,"modifiedBy":"15032609451","modifiedTime":modifiedTime}
            else:

                params = {"id":id,"alias":alias,"phoneNumber":phoneNumber,"agentType":agentType,"tenantId":tenantId,"modifiedBy":"15032609451","modifiedTime":modifiedTime}
            try:
                response = req.request(method='POST',params=params, url=url, headers=header, timeout=10, proxies=None)
                print(response.content)
                print "asd"
            except Exception as e:
                print("请求失败")
            #print type(json.loads(response.content)) #//dict
            # if json.loads(response)["retCode"] =="000000":
            #     print("同步成功,结束")
            #     return response
            # else:
            #     print "同步未成功,请核查原因"

        else:
            print "请求参数信息为空，同步请求失败"
            return








if __name__ == '__main__':

    run = runMe()
    history = open("historyNo", "a")
    try:
        phone = str(sys.argv[1]).strip()
        print "phone is " + phone
        result = run.toRequest(phone)
        history.write(phone)
    except Exception as e:
        print("无参执行脚本，读取号码文件")
        with open('loginNo', 'r') as f:
            lala = f.readlines()
            for phone in lala:
                result = run.toRequest(phone.strip())
                history.write(phone)