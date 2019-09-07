#coding:utf8

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
from Utils import getDate, printLog,createPath
from conf import config
import sendmessage
import time
loggerTime=getDate.getTime()
today =getDate.getToday()
log_dir = createPath.mkdir(config.log_dir)
phone_dir =createPath.mkdir(config.data_dir+'/duanxin')
logger = printLog.getLogger(loggerTime, 'duanxin', log_dir)

messageFile = config.conf_dir + '/message.conf'
resultFile = '%s/sent_%s.txt' % (phone_dir, loggerTime)
failedFile = '%s/unsend_%s.txt'% (phone_dir, loggerTime)

def getData():
    '''
    读取号码文件
    :param batch_phone_file:
    :return:
    '''
    files =os.popen('ls %s/*%s.dat'%(phone_dir,today) )
    filelist = files.readlines()

    batchDict ={}
    for file in filelist :
        file =file.strip()
        with open('%s' % (file), 'r') as batch_phones:
            count = 0
            for line in batch_phones:
                count += 1
                batch_phone = line.split(",")
                batch = batch_phone[0].strip()
                phone = batch_phone[1].strip()
                batchDict[batch]= batchDict.get(batch,[])
                batchDict[batch].append(phone)
            logger.info("读取 批次对应号码 完成  计数：[%s : %s ]"%(file,count))
    return  batchDict
    # test= {
    #      '1024944252335034368': ["15032609451"],
    #      '1026372835116978176a': ["15032609451"],
    #      '1026374686516318208': ["15032609451"]
    #             }
    # return test
def getMessage(messagefile):
    messageDict ={}
    with open('%s' % (messagefile), 'r') as messages:
        for line in messages:
            batch_meassage = line.split("|")
            '''
            待确认参数
           '''
            batch = batch_meassage[0]
            message = batch_meassage[5]
            messageDict[batch] =message
    return  messageDict

def sendMessage():
    batch_phone_dict =getData()
    message_dict = getMessage(messageFile)
    success_count = 0
    fail_list = []
    #这里也要加try
    for batchList in batch_phone_dict.items():
        try:
            batch_id = batchList[0]
            for phone in batchList[1]:
                phone = phone
                try:
                    mesage_content = message_dict[batch_id]
                except Exception as e:
                    fail_list.append("%s,%s,0"%(batch_id,phone))
                    logger.error("获取号码批次的短信内容失败:%s %s"%(batch_id,phone))
                    #这里要写入文件：号码—批次
                    continue
                print ("%s  %s %s"%(batch_id,phone,mesage_content))

                try:
                    sendmessage.send(phone, mesage_content, resultFile)
                    print ("Sent --- %s :%s" % (phone, mesage_content))
                    success_count += 1
                except Exception :
                    fail_list.append("%s,%s"%(batch_id,phone))
                    logger.error("短信发送异常: %s,%s,1"%(batch_id,phone))

        except Exception as e:
            logger.error(e.message)
            logger.error("该批次短信发送异常：%s" %(batchList[0]))
    unsendFile =open(failedFile, "a")
    for i in range(fail_list.__len__()):
        unsendFile.write('%s\n'%(fail_list[i]))


    print("短信发送结束: 响应成功 [ %s ]，详情见%s ,失败 [ %s],详情见%s" %(success_count,resultFile,fail_list.__len__(),failedFile))






if __name__ == "__main__":

    #sendMessage()
    history1 = open('history', "r")
    commond = raw_input("最新执行完成时间是%s,是否要进行短信下发:[yes],[no]? "%(history1.readlines()))
    if commond.strip() =='yes':
        sendMessage()
    elif commond.strip() =='no':
        exit("已终止")
    else :
        exit("哎")
    history2 = open('history', "w")
    time =time.strftime('%Y%m%d %H:%M:%S',time.localtime(time.time()))
    history2.write(time)
    history2.write('\n')