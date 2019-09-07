#coding:utf8

#对账日期，为空时，默认对账日期为T-1
#check_date = '20180501'
check_date = '20180505'
#对账文件存放目录
file_dir = '/echnweb/tongji_data/dz2_0/res_data'
#file_dir = '/Users/hsc/PycharmProjects/tools/dz2_0/res_data'

#渠道编码对应字典
channel_dict = {
    '11': '6900',
    '00': '0702',
    '0003': '0705',
    '12':'0705'
}

#配置sftp连接信息，目前是流量对账文件for绿点
conn_sftp_data = {
    'local_path':'/echnweb/tongji_data/dz2_0/res_data/',
    'remote_path':'pcyewu',
    'username':'pcyewusftp',
    'password':'Pcyewusftp&*()_12323',
    'host':'172.16.137.93',
    'port':8889,
}


prov_code = ['100','200','210','220','230','240','250','270','280','290','311',
             '351','371','431','451','471','531','551','571','591','731','771',
             '791','851','871','891','898','931','951','971','991']
