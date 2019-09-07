# _*_ coding:utf-8 _*

import sys
import paramiko
import os
import logging
import logging.config
import shutil
import re

topLevelDirectory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print topLevelDirectory
sys.path.append(topLevelDirectory)  # 为了导入平级的模块
from configModule    import getconfig


# 实现单个文件上传
def sftp_upload(host, port, username, password, localfile, remotepath):
    try:
        logger = logging.getLogger("accountLog")
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(sf)
        if os.path.isfile(localfile):  # 判断是否文件
            sftp.put(localfile, remotepath+os.path.basename(localfile))  # 上传文件
            logger.debug('[%s] upload finished!', localfile)
        else:
            logger.error('[%s] is not a file!', localfile)
    except Exception, e:
        logger.error('upload exception: [%s]', e)
    finally:
        sf.close()

# 单个文件下载
def sftp_download(datestr, host, port, username, password, localpath, remotefile):
    try:
        logger = logging.getLogger("accountLog")
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(sf)
        if os.path.isdir(localpath):  # 判断本地参数是目录还是文件
            sftp.get(remotefile, localpath+os.path.basename(remotefile))  # 下载目录中文件
            logger.debug('[%s] download finished!', remotefile)
            checkfilefmt(localpath+os.path.basename(remotefile), datestr)
        else:
            logger.error('[%s] is not a path!', localpath)
    except Exception, e:
        logger.error('download exception: [%s]', e)
    finally:
        sf.close()

# 批量下载
def sftp_download_all(datestr, host, port, username, password, localpath, remotepath):
    try:
        logger = logging.getLogger("accountLog")
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(sf)
        if os.path.isdir(localpath):  # 判断本地参数是目录还是文件
            for f in sftp.listdir(remotepath):  # 遍历远程目录
                rule = "account_[a-z]*2coupons_" + datestr + "_[0-9]{3,7}_[0-9]{3}.dat"  # 只下载指定账期的文件
                if re.match(rule, f):
                    sftp.get(os.path.join(remotepath + f), os.path.join(localpath + f))  # 下载目录中文件
                    logger.debug("[%s%s] download finished!", localpath, f)
                    checkfilefmt(os.path.join(localpath + f), datestr)
        else:
            logger.error('[%s] is not a path!', localpath)
    except Exception, e:
        logger.error('download exception: [%s]', e)
    finally:
        sf.close()

# 批量上传
def sftp_upload_all(host, port, username, password, localpath, remotepath):
    try:
        logger = logging.getLogger("accountLog")
        sf = paramiko.Transport((host, port))
        sf.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(sf)
        if os.path.isdir(localpath):  # 判断是否目录
            for f in os.listdir(localpath):  # 遍历本地目录
                sftp.put(os.path.join(localpath + f), os.path.join(remotepath + f))  # 上传目录中的文件
                logger.debug('[%s%s] upload finished!', remotepath, f)
        else:
            logger.error('[%s] is not a path!', localpath)
    except Exception, e:
        logger.error('upload exception: [%s]', e)
    finally:
        sf.close()

# ssh 执行命令
def ssh2(ip,username,passwd,cmd):
    try:
        logger = logging.getLogger("accountLog")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, 8822, username, passwd, timeout=5)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        #屏幕输出
        for o in stdout.readlines():
            logger.info("[%s]", o)
        logger.debug("ip=[%s], cmd=[%s], exec finished!",ip, cmd)
        ssh.close()
    except Exception, e:
        logger.error('upload exception: [%s]', e)


# 根据账期获取全部对账文件
def get_account_file(account_date, type='all', prov='999'):
    logger = logging.getLogger("accountLog")
    # 获取充值对账文件
    if type == 'all' or type == 'recharge':
        logger.debug(u"开始获取充值对账文件，账期[%s]，省份[%s]", account_date, prov )
        host, port, usrname, password, localpath, download, upload = getconfig.readsftpinfo('recharge_sftp')
        if os.path.isdir(localpath):
            local = localpath + "recharge/" + str(account_date) + "/"
            # local = localpath + "recharge\\" + str(account_date) + "\\"
            if prov == '999':
                # 下载全部文件
                if os.path.exists(local):  # 如果目录存在则全部清除
                    shutil.rmtree(local)
                os.makedirs(local)
                sftp_download_all(account_date, host, int(port), usrname, password, local, download)
            else:
                # 下载指定省份文件
                if not os.path.exists(local):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                    os.makedirs(local)
                filename = "account_recharge2coupons_"+str(account_date)+"_"+prov+"_001.dat"
                sftp_download(account_date, host, int(port), usrname, password, local, download+filename)
        else:
            logger.error("recharge_sftp配置错误，[%s]不存在！", localpath)

    # 获取流量对账文件
    if (type == 'all' or type == 'flow'):
        logger.debug("开始获取流量对账文件，账期[%s]，省份[%s]", account_date, prov)
        host, port, usrname, password, localpath, download, upload = getconfig.readsftpinfo('flow_sftp')
        if os.path.isdir(localpath):
            local = localpath + "flow/" + str(account_date) + "/"
            # local = localpath + "flow\\" + str(account_date) + "\\"
            if (prov == '999'):
                # 下载全部文件
                if os.path.exists(local):  # 如果目录存在则全部清除
                    shutil.rmtree(local)
                os.makedirs(local)
                sftp_download_all(account_date, host, int(port), usrname, password, local, download)
            else:
                # 下载指定省份文件
                if not os.path.exists(local):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                    os.makedirs(local)
                filename = "account_flow2coupons_" + str(account_date) + "_" + prov + "_001.dat"
                sftp_download(account_date, host, int(port), usrname, password, local, download + filename)
        else:
            logger.error("flow_sftp配置错误，[%s]不存在！", localpath)

    # 获取商品对账文件
    if (type == 'all' or type == 'commodity'):
        logger.debug("开始获取商品对账文件，账期[%s]，省份[%s]", account_date, prov)
        host, port, usrname, password, localpath, download, upload = getconfig.readsftpinfo('commodity_sftp')
        if os.path.isdir(localpath):
            local = localpath + "commodity/" + str(account_date) + "/"
            # local = localpath + "commodity\\" + str(account_date) + "\\"
            if (prov == '999'):
                # 下载全部文件
                if os.path.exists(local):  # 如果目录存在则全部清除
                    shutil.rmtree(local)
                os.makedirs(local)
                sftp_download_all(account_date, host, int(port), usrname, password, local, download)
            else:
                # 下载指定省份文件
                if not os.path.exists(local):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                    os.makedirs(local)
                filename = "account_commodity2coupons_" + str(account_date) + "_" + prov + "_001.dat"
                sftp_download(account_date, host, int(port), usrname, password, local, download + filename)
        else:
            logger.error("commodity_sftp，[%s]不存在！", localpath)

    # 获取商品对账文件
    if (type == 'all' or type == 'package'):
        logger.debug("开始获取套餐对账文件，账期[%s]，省份[%s]", account_date, prov)
        host, port, usrname, password, localpath, download, upload = getconfig.readsftpinfo('package_sftp')
        if os.path.isdir(localpath):
            local = localpath + "package/" + str(account_date) + "/"
            # local = localpath + "package\\" + str(account_date) + "\\"
            if (prov == '999'):
                # 下载全部文件
                if os.path.exists(local):  # 如果目录存在则全部清除
                    shutil.rmtree(local)
                os.makedirs(local)
                sftp_download_all(account_date, host, int(port), usrname, password, local, download)
            else:
                # 下载指定省份文件
                if not os.path.exists(local):  # 如果是单个文件下载，则不清除目录，还需要判断目录是否存在
                    os.makedirs(local)
                filename = "account_package2coupons_" + str(account_date) + "_" + prov + "_001.dat"
                sftp_download(account_date, host, int(port), usrname, password, local, download + filename)
        else:
            logger.error("package_sftp，[%s]不存在！", localpath)

# 检查下载的文件是否符合要求
def checkfilefmt(filename, datestr):
    logger = logging.getLogger("accountLog")
    if not os.path.exists(filename):
        logger.error("download file [%s] do not exists!", filename)
        return False
    if os.path.getsize(filename) == 0:
        logger.error("download file [%s] size is 0!", filename)
        return False
    rule = "account_[a-z]*2coupons_[0-9]{8}_[0-9]{3,7}_[0-9]{3}.dat"
    if not re.match(rule, os.path.basename(filename)):
        logger.error("download file [%s] name error!not match [%s]!", filename, rule)
        return False
    if (filename.split("_")[2] != datestr) :
        logger.error("accout date error filanme [%s] date [%s]!", filename, datestr)
        return False
    fp = open(filename, "rb+")
    fistline = fp.readlines() #读取全部文件，判断行数是否正确，文件过大时可能存在问题，后续可以优化
    if len(fistline[0].split("|")) != 4:
        logger.error("filanme [%s] check error [%s]!", filename, fistline[0])
        fp.close()
        return False
    if (fistline[0].split("|")[0] != "111" or int(fistline[0].split("|")[3].strip()) != len(fistline) - 2 ):
        print len(fistline)
        logger.error("filanme [%s] check error line num [%s] error!", filename, fistline[0].split("|")[3].strip())
        fp.close()
        return False
    return True


if __name__ == '__main__':
    logging.config.fileConfig("e:\\log.config")
    host = '127.0.0.1'  # 主机
    port = 8822  # 端口
    username = 'e3base'  # 用户名
    password = "XHyC8qKrpRqN"  # 密码
    local = 'E:\\sftptest\\'  # 本地文件或目录，与远程一致，当前为windows目录格式，window目录中间需要使用双斜线
    remote = '/e3base/test/hanyf/testsftp/'  # 远程文件或目录，与本地一致，当前为linux目录格式
    # sftp_upload(host, port, username, password, local, remote)  # 上传
    # sftp_download(host, port, username, password, local, remote)#下载
    # sftp_download_all(host, port, username, password, local, remote)
    # get_account_file('20180213')
    checkfilefmt("E:\\sftptest\\account_recharge2coupons_20180214_100_001.dat","20180214")
    #ssh2(host, username, password, "ls -la")
