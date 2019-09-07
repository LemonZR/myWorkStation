#!/e3base/python/Python-2.7.6/python
#coding:utf8

import os,sys
from myException import myException


hadoopBaseDir = '/DATA_APP/pods/08serv/Coupons/V2'

def path(type='',year='',month='',day='',part='part-m-00000'):
    try:
        if type == 'card':
            path='%s/td_pcard_info/yearstr=%s/monthstr=%s/daystr=%s/db=echnmarketdb*331*/phoneTail=*/%s' %(hadoopBaseDir,year,month,day,part)
            return  path
        elif type == 'batch':
            path = '%s/td_def_pcard_batch/yearstr=%s/monthstr=%s/daystr=%s/%s' %(hadoopBaseDir, year, month, day, part)
            return path
        else:
            return ''
    except Exception :
        raise
def hadoopToFile(cmd,filePath):
    cmd = cmd.strip()
    command = '%s >> %s' % (cmd, filePath)
    try:
        print command
        a=os.system(command)

        if a == 0:
            return a
        else:
            return 1
    except Exception as e:

        raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
def hadoopToFileQ(cmdQueue,filePath):
    while True:
        try:
            cmd=cmdQueue.get(block=False)
            try:
                command = '%s >> %s' % (cmd, filePath)
                print command
                os.system(command)
            except Exception as e:
                raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
        except Exception as e:

            raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)
def hadoopToFileL(cmdList,filePath):

    for cmd in cmdList:
        try:
            command = '%s >> %s' % (cmd, filePath)
            os.system(command)

        except Exception as e:
            raise myException(e, sys.argv[0], sys._getframe().f_back.f_lineno)

# hadoop fs -cat /DATA_APP/pods/08serv/Coupons/V2/td_def_pcard_batch/yearstr=2019/monthstr=08/daystr=05/part-m-* |awk -F '$' '{if($51==0&&$6==3)print $1,$10,$9,$2,$5,$33,$32,$36}'
#hadoop fs -cat
# /DATA_APP/pods/08serv/Coupons/V2/td_def_pcard_batch/yearstr=2019/monthstr=08/daystr=05/part-m-00000 |awk -F '$' '{if($51==0&&$6==3)print $1"|"$10"|"$9"|"$2"|"$5"|"$33"|"$32"|"$36}'

#hadoop fs -cat
#  hadoop fs -cat /DATA_APP/pods/08serv/Coupons/V2/td_pcard_info/yearstr=2019/monthstr=07/daystr=10/db=echnmarketdb*331*/phoneTail=*/part-m-00000 |grep 1148528516950786048
#  |awk -F '|' '{if(substr($8,0,10)=="2019-08-14"&&$12=="8")print 1148528516950786048"|"$19}'
def fun():
    pass
if __name__=='__main__':
    try:
        a = hadoopToFile('sdir','123')
        print a
    except Exception as e:
        print e
    import Queue
    q=Queue.Queue()
