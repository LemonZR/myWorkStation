#coding:utf8
def qryData(cur,sql):
    head, body = qry(cur, sql)
    # 调用数据处理模块，将数据库查询出的数据格式化为可插入excel表格的数据
    excel_data = conver(head, body)
    resbody = excel_data["body"]
    resHead = excel_data["head"]
    return resHead,resbody

def connect_mysql(conn_data):
    import pymysql

    conn = pymysql.Connect(host=conn_data['host'], port=conn_data['port'], user=conn_data['user'],
                           passwd=conn_data['passwd'], db=conn_data['db'], charset='utf8')
    '''
   conn = pymysql.Connect(host='10.255.202.113', port='3312', user='echnmarket',
                         passwd='EnMket2~', db='echnmarketdb', charset='utf8')
   '''
    cur = conn.cursor()
    return cur, conn


def conn_db(conn_data):
    '''
    连接数据库
    :param conn_data:数据库连接配置，在config.py文件中
    :return: 返回数据库连接cur
    '''
    try:
        if conn_data['SERVER'] == 'mysql':
            cur,conn = connect_mysql(conn_data)
            return cur,conn
        elif conn_data['SERVER'] == 'sybase':
             import  sybpydb
             conn = sybpydb.connect(user=conn_data['user'], password=conn_data['passwd'], servername=conn_data['db'])
             cur = conn.cursor()
             return cur,conn
    except Exception as e:
        print(e.__str__()+"\nUsing default :mysql-database...")


def conn_db_new(conn_data):

     import  mysql.connector

     conn = mysql.connector.connect(**conn_data)
     '''
    conn = pymysql.Connect(host='10.255.202.113', port='3312', user='echnmarket',
                          passwd='EnMket2~', db='echnmarketdb', charset='utf8')
    '''
     cur = conn.cursor()
     return cur,conn


def qry(cur,sql):

    try:
        cur.execute(sql)
    except Exception as e:
        print sql+"qry:shibai "
        print(e)
    head = cur.description#查询表头
    #print head
    body = cur.fetchall()#查询数据
    #print body
    return head, body#返回查询结果表头及表数据，元组类型数据

def conver(head, body):
    '''
    格式转换函数，将数据库查询的数据转换为可插入excel数据
    :param head: 数据头
    :param body: 数据体
    :return:excel_data_format格式化后的数据，可插入excel表格
    '''
    excel_data_format={'head':[],'body':[]}

    # 生成表头，遍历head
    for col in range(0, len(head)):

        excel_data_format['head'].append(head[col][0])

    # 生成表数据，遍历body
    for row in range(0, len(body)):
        excel_data_format['body'].append([])
        for col in range(0, len(body[row])):
            excel_data_format['body'][row].append(body[row][col])
    return excel_data_format
