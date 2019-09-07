#coding:utf8

import os,sys
import datetime,time

import requests
import shutil

#print time.strftime('%Y%m%d', time.localtime(time.time()))

reload(sys)
sys.setdefaultencoding("utf-8")

import csv

from conf import dz_conf
from conf import operate_id_conf


#限制的活动id
operate_id_list = operate_id_conf.operate_id

# 1、os.path.exists(path) 判断一个目录是否存在
# 2、os.makedirs(path) 多层创建目录
# 3、os.mkdir(path) 创建目录

def create_file_dir(fileDir,checkDate):
    '''
    创建对账文件当日目录
    :return:
    '''
    file_dir = '%s'%(fileDir)
    #判断所对账账期文件目录是否存在
    dir_exists(file_dir)
    print "对账文件目录：%s"%file_dir
    return file_dir

def dir_exists(file_dir):
    '''
    判断对账文件目录是否存在
    :param file_dir:文件目录
    :return:
    '''
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    else:
        if not os.path.exists(file_dir+'bak'):
            shutil.copytree(file_dir, file_dir+'bak')
        shutil.rmtree(file_dir)
        print 'old file remove success ，ok！'
        os.mkdir(file_dir)

def create_csv(excel_data,checkDate):

    #获取对账文件存放目录
    fileDir = dz_conf.file_dir
    #生成对账文件存放目录（处理一下）
    file_dir = create_file_dir(fileDir,checkDate)
    #获取省份编码
    prov_code_list = dz_conf.prov_code

    #渠道字典
    channel_dict = dz_conf.channel_dict

    # 文件体的表头（数据库内容）---------------
    table_head = excel_data['head']

    # 写入文件体的表头，文件体的表数据
    table_body = excel_data['body']


    #对账前，拿出所有当前对账账期的文件名并删除
    files = file_name_get(file_dir)

    #用账期把文件分出来
    files_now_list = []
    for file in files:
        if checkDate in file:
            files_now_list.append(file)

    for file_one in files_now_list:
        os.remove('%s/%s'%(file_dir,file_one))


    #--------写入文件体----------#
    #循环文件体数据
    for line in table_body:
        #根据文件一行内容，生成文件名
        file_name = create_file_name(line,channel_dict)
        #格式化文件体，逐行格式化，生成固定格式,列表[一行数据，]
        line_list = create_file_body(line,operate_id_list)
        if line_list == []:continue
        #把文件体逐行写入对应文件
        write_csv(line_list, file_name,file_dir)


    # --------写入文件头----------#
    #获取文件目录下文件名，为了通过文件名分别写入文件头
    files = file_name_get(file_dir)

    #用账期把文件分出来
    files_now_list = []
    for file in files:
        if checkDate in file:
            files_now_list.append(file)

    #循环获取的文件名
    # （获取文件头需要参数--》文件名，文件头(文件首行：通过首行信息得出文件头内容，文件内容行数),文件目录）
    for filename in files_now_list:
        #打开文件
        with open('%s/%s'%(file_dir,filename),'r') as csvfile:
            #获取文件内容，列表形式
            file_content_list=csvfile.readlines()


            #获取文件行数
            file_line_count = len(file_content_list)

            #获取文件首行
            first_line = file_content_list[0]

            #生成文件头
            file_head = create_file_head(first_line,file_line_count)
            #插入文件头
            insert_file_head(filename,file_head,file_dir)
            insert_file_end(filename,file_dir,file_line_count)

    #如果某些省份没有订单，验证一下，需要生成空文件
    files = file_name_get(file_dir)

    #用账期把文件分出来
    files_now_list = []
    for file in files:
        if checkDate in file:
            files_now_list.append(file)

    # 判断是否所有省份都有文件
    if verification_prov(files_now_list,prov_code_list):
        #无文件省份列表
        prov_empty_list = verification_prov(files_now_list, prov_code_list)
        provnum = len(prov_empty_list)
        prov_empty_str = ','.join(prov_empty_list)
        print '无记录省份为：【%s】，为这【%s】个省份补充空文件上传'%(prov_empty_str,provnum)
        #迭代无文件省份，生成新文件
        for prov_empty in prov_empty_list:
            # 根据文件一行内容，生成文件名
            file_name = create_file_name('', '%s|%s'%(checkDate, prov_empty))
            # 生成文件头
            file_head = create_file_head('', 0)
            # 插入文件头
            insert_file_head(file_name, file_head, file_dir)
            insert_file_end(file_name, file_dir, 0)
    else:
        print '所有省份均有记录'

def verification_prov(files,prov_code_list):
    '''
    判断是否所有省份都有文件
    :param file_line_count: 文件行数
    :return:
    '''
    #account_recharge2coupons_20180330_100_001.dat
    file_prov_list = []
    for file in files:
        file_prov=file.split('_')[3]
        file_prov_list.append(file_prov)

    #筛选没有哪些身份
    empty_prov = list(set(prov_code_list) - set(file_prov_list))
    if len(empty_prov) > 0:
        return empty_prov
    else:
        return []
def insert_file_head(file_name,file_head,file_dir):
    '''
    插入文件头
    :param file_name:文件名，此处为已生成且文件体已完整文件
    :param file_head:文件头，此处为根据文件首行生成的文件头
    :return:无
    '''
    if os.path.exists('%s/%s'%(file_dir,file_name)):
        #打开文件
        with open('%s/%s'%(file_dir,file_name),'r') as fp:
            #获取文件内容，字符串形式
            str = fp.read()
            #拼接文件头
            str = file_head + "\n" + str
        fp.close()
        with open('%s/%s'%(file_dir,file_name), "wb") as fp:
            #拼接后文件插回原文件名文件
            fp.write(str)
        fp.close()
    else:
        with open('%s/%s'%(file_dir,file_name), "wb") as fp:
            #拼接后文件插回原文件名文件
            fp.write(file_head + "\n")
        fp.close()
def file_name_get(file_dir):
    '''
    获取文件目录下文件名
    :param file_dir:文件目录
    :return: files，目录下文件名，列表形式
    '''
    for root, dirs, files in os.walk(file_dir):
        #print(root)  # 当前目录路径
        #print(dirs)  # 当前路径下所有子目录
        #print(files) # 当前路径下所有非目录子文件
        return files
def write_csv(data,file_name,file_dir):
    '''
    写入csv文件
    :param data: data is list一行数据，格式:[一行数据,]，一个元素为一行内容
    :param file_name: 文件名
    :param file_dir: 文件目录
    :return: 无
    '''
    with open('%s/%s'%(file_dir,file_name), 'a+') as csvfile:
        spamwriter = csv.writer(csvfile,dialect='excel')
        #插入内容
        if len(data) == 1:
            spamwriter.writerow(data)
        elif len(data) > 1:
            #写入文件体
            for line in data:
                spamwriter.writerow(line)
        else:
            print "call write_csv(data,file_name) :data error or None"
def create_file_body(data,operate_id_list):
    '''
    生成文件体插入csv文件字符串（每次处理一行）
    :param data: 文件体列表（每一行）
    :param channel_dict: 渠道字典（配置文件中的）
    :return: 返回插入csv文件到字符串（一行）
    '''
    #创建空字符串
    result = ''
    #创建空列表
    file_line_list = []

    #根据活动筛选
    if str(data[-1]) not in operate_id_list:
        del (data[-1])
        #迭代传入的文件体列表（一行）
        for i in range(len(data)):
            if i == 0:
                one_data = data[i].strftime('%Y%m%d')
            elif i == 2:
                one_data = data[i][:16]
            elif i == 5:
                if str(data[i]) in ['0','2', '5','6','8','10','12','14']:
                    one_data = '8'
                elif str(data[i]) == '4':
                    one_data = '10'
                else:
                    one_data = data[i]
            else:
                #给取出的列表元素定义变量
                one_data = data[i]
            #排除None字符串，改为''
            if one_data == None:
                one_data = ''
                result += one_data + '|'
            else:
                #将列表迭代后生成字符串，指定分隔符为"|"
                result += str(one_data) + '|'#str（）是将数据转化为字符串，因为其他数据类型不能和字符串相加
        #因插入csv需要是列表插入，所以将字符串插入列表
        file_line_list.append(result[:-1])
        #返回文件体字符串列表（一行）
        return file_line_list
    else:return []
def create_file_head(first_line,file_line_count):
    line = first_line.split('|')
    head_id = '111'#头记录标记
    file_version = '01'#渠道标识
    #file_code = '001'#文件编码
    #file_count = '001'#文件总数
    file_create_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')#文件产生时间
    file_line_count = str(file_line_count).zfill(6)#文件中记录总数
    #文件头
    #file_head = "%s%s%s%s%s%s"%(head_id,channel_id,file_code,file_count,file_create_time,file_line_count)
    file_head = "%s|%s|%s|%s"%(head_id,file_create_time,file_version,file_line_count)
    return file_head
def insert_file_end(file_name,file_dir,file_line_count):
    #插入文件尾
    #line = first_line.split('|')
    end_line = '999'#头记录标记
    file_version = '01'#渠道标识
    file_count = '001'#文件总数
    file_count = '001'#文件总数
    file_create_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')#文件产生时间
    file_line_count = str(file_line_count).zfill(6)#文件中记录总数
    #文件尾
    file_end = "%s|%s|%s|%s"%(end_line,file_create_time,file_version,file_line_count)
    with open('%s/%s'%(file_dir,file_name), "a") as fp:
        fp.write(file_end + "\n")
    fp.close()
def create_file_name(line,checkDate_prov_empty):

    if line:
        pay_date = line[0].strftime('%Y%m%d')
        #省份编码20180326
        prov_code = line[4]
        # 文件编码
        # file_code = n.zfill(3)
        file_code = '001'
        # file_name = 'giveflow_%s_%s_%s_%s_%s.dat'%(pay_date,channel_code,prov_code,marketId,file_code)
        file_name = 'account_recharge2coupons_%s_%s_%s.dat' % (pay_date, prov_code, file_code)
        # file_name = 'giveflow_20171102_6900_451_10012_001.dat'
        return file_name
    else:
        pay_date = checkDate_prov_empty.split('|')[0]
        prov_code = checkDate_prov_empty.split('|')[1]
        file_code = '001'
        # file_name = 'giveflow_%s_%s_%s_%s_%s.dat'%(pay_date,channel_code,prov_code,marketId,file_code)
        file_name = 'account_recharge2coupons_%s_%s_%s.dat' % (pay_date, prov_code, file_code)
        # file_name = 'giveflow_20171102_6900_451_10012_001.dat'
        return file_name
def create_file_body_head():
    body_head='transactionId|chargeCell|settleDate|channel|orderCode|flowType|flowNum|giveTime|marketId'
    res = [body_head]
    return res
