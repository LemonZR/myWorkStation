#coding:utf8
import  time,datetime

def getToday():
    return  time.strftime('%Y%m%d',time.localtime(time.time()))
def getTime():
    return  time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
def getMMtime():
    mm = int(time.time()*1000).__str__()[-3:]
    t = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    return t+mm

def getYesterday():
    yesterday = datetime.datetime.today() + datetime.timedelta(-1)
    yesterday = yesterday.strftime('%Y%m%d')
    return yesterday
if __name__=='__main__' :
   print getMMtime()
   print getTime()