#!/tongweb/bin/python/python2.7/bin/python2.7
#coding:utf8

import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog, dbOper
from conf import config

loggerDate = getDate.getMMtime()
logger=printLog.getLogger(loggerDate,"test","%s/ecora2card/log/"%BASE_DIR)


import sql_conf

def getConnect(connect_info):
    try:
        cur,conn = dbOper.conn_db(connect_info)
        logger.info("连接数据库成功")
    except Exception as e:
        logger.info(e)
        logger.error("连接数据库失败")
        cur,conn = None,None
    return cur,conn

def gen_insert_sql(sybase_date):
    sql_list =[]
    file_name = os.sep.join([BASE_DIR ,"ecora2card","data",sybase_date])

    with open(file_name,'r') as valueline:
        for values in valueline.readlines():
            valuestr =','.join(values.strip().split('\t'))
            sql = sql_conf.inser_sql % (valuestr)
            sql_list.append(sql)
    valueline.close()
    return sql_list


def gen_pcard_act_rel_data(connect_info,pcard_act_rel_data):
    file_name = os.sep.join([BASE_DIR ,"ecora2card","data",pcard_act_rel_data])
    sql =[sql_conf.pcard_act_sql]
    result =query(connect_info,sql,file_name)
    return result

def gen_sybase_select_sql(pcard_act_rel_data):
    sql_model =sql_conf.sybase_sql_test
    sql_list =[]
    file_name = os.sep.join([BASE_DIR ,"ecora2card","data",pcard_act_rel_data])
    print "open %s" %file_name

    with open(file_name,'r') as pcardid_actid:
        for line in pcardid_actid:
            pcard_id,act_id =line.strip().split('\t')
            # act_id =act_id.split("'")[1]
            # pcard_id =pcard_id.split("'")[1]
            sql = sql_model %(pcard_id,act_id)
            sql_list.append(sql)
    pcardid_actid.close()
    return sql_list

def gen_sybase_data(connect_info,sybase_data_file_name,pcard_act_rel_data_file_name):
    sqls = gen_sybase_select_sql(pcard_act_rel_data_file_name)
    file_name = os.sep.join([BASE_DIR, "ecora2card", "data", sybase_data_file_name])
    result = query(connect_info,sqls,file_name)
    return result

def insert(connect_info,sqls,insert_resultfile_name):
    cur, conn = getConnect(connect_info)
    file_name = os.sep.join([BASE_DIR ,"ecora2card","data",insert_resultfile_name])
    resultfile =open(file_name,"a")
    success_count = 0
    fail_count = 0
    for sql in sqls:
        try:
            cur.execute(sql)
            conn.commit()
            success_count +=1
            logger.info("[成功] %s  " % sql )
            resultfile.write("%s [成功]\n" %(sql))
        except Exception as e:
            fail_count +=1
            logger.error(" [失败] %s  %s" %(sql,e))
            resultfile.write("%s  [失败]%s\n" %(sql,e))
    try:
        #conn.close()
        logger.info(" insert finished ,conn closed")
    except Exception as e:
        logger.error(e)
    return "插入成功数 [%s] 插入失败数 [%s]"%(success_count,fail_count)


def query(connect_info,sql_list,qry_resultfile_name):

    resultfile =open(qry_resultfile_name,"w")
    print qry_resultfile_name
    miss =open(qry_resultfile_name+"_miss","w")
    cur, conn = getConnect(connect_info)
    not_null_count = 0
    null_count = 0
    result_line_count = 0
    for sql in sql_list:
        try:
            qryHead,qryResult = dbOper.qryData(cur, sql)
            print "woshi sql "+sql
            resultbody = []
            if qryResult.__len__():
                not_null_count +=1
                result_line_count +=qryResult.__len__()
                for i in range(0,qryResult.__len__()):
                    #查询结果
                    resultbody.append([]) #长度+1
                    for ii in range(0,len(qryResult[i])):
                        if qryResult[i][ii] is None:
                            qryResult[i][ii] = 'null'
                        resultbody[i].append("'%s'"%qryResult[i][ii])
                for i in range(0,len(resultbody)):
                    resultfile.write('\t'.join(resultbody[i])+"\n")
            else:
                logger.error("查询结果为空:%s"%sql)
                null_count +=1
                miss.write(sql+'\n')
        except Exception as e:
            logger.error("%s \n--------执行失败"%sql)
            logger.error(e)
    try:
        resultfile.close()
        miss.close()
        #conn.close()
        logger.info(" query finished ,conn closed")

    except Exception as e:
        logger.error(e)
    try:
        return  "执行查询次数 [%s],结果数 [%s],结果总行数 [%s],无结果数 [%s]  " %(len(sql_list),not_null_count,result_line_count,null_count)
    except Exception as e:
        return"查询异常 %s" %e




if __name__=='__main__':
    pcard_act_rel_data_file_name ='test_mysql'
    sybase_data_file_name ='test_sybase'
    card_qry_result = gen_pcard_act_rel_data(config.local_mysql,pcard_act_rel_data_file_name)
    sybase_result =gen_sybase_data(config.local_mysql,sybase_data_file_name,pcard_act_rel_data_file_name)
    insert_sql_list = gen_insert_sql(sybase_data_file_name)
    print insert_sql_list
    insert_result = insert(config.local_mysql,insert_sql_list,'insert_result_filename')
    print sybase_result
    #logger.info("%s%s"%(qry_result,insert_resutl))
