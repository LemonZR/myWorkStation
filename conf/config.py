#coding:utf8

#-------------------sybase--------------------#
#一期生产库
import os,sys
ecora={'SERVER':'sybase',
      'host':'',
      'port':'',
      'user':'echnuser',
      'passwd':'aHc4#y+EC3]qsObX4RA@',
      'db':'ECORA',
      'charset':'utf8',
      }

#二期生产库
ecora2={'SERVER':'sybase',
      'host':'',
      'port':'',
      'user':'echnuser',
      'passwd':'Km3H]8ECORA@5sny',
      'db':'ECORA2',
      'charset':'utf8',
      }

#用户中心库（25600）查吐槽
usrdb={'SERVER':'sybase',
      'host':'',
      'port':'',
      'user':'echnuser',
      'passwd':'m]Y8bX4[USRDB@3f',
      'db':'USRDB',
      'charset':'utf8',
      }

#查询库 查接口访问
qrydb={
      'SERVER':'sybase',
      'host':'',
      'port':'',
      'user':'echnuser',
      'passwd':'aHc4#y+QRYDB]72c',
      'db':'QRYDB',
      'charset':'utf8',
}
#---------------------mysql-------------------------#

#在线客服
zxkf={
      'SERVER':'mysql',
      'host':'10.255.202.115',
      'port':8066,
      'user':'ydsc',
      'passwd':'ydsc_2016!',
      'newpasswd':'i[4Df9kdA3mYH$XJvX-bGWXJ5',
      'db':'ydsc',
      'charset':'utf8'
      }
#评价中心
pjzx={
      'SERVER':'mysql',
      'host':'10.255.202.71',
      'port':8066,
      'user':'comment',
      'passwd':'comment_2016!',
      'db':'comment',
      'charset':'utf8'
      }
#卡券中心
kqzx={
      'SERVER':'mysql',
      'host':'10.255.201.225',
      'port':3309,
      'user':'echnmarketdb',
      'passwd':'OPtD4twn0Nk',
      'db':'echnmarket1',
      'charset':'utf8'
      }
#卡券中心-测试
kqzx_test={'SERVER':'mysql',
      'host':'10.255.201.198',
      'port':3308,
      'user':'echnmarket2',
      'passwd':'5$^y$]X+94a]K[)q5@Z]gz~n6',
      'db':'echnmarketdb2',
      'charset':'utf8',
      }
#卡券中心2.0_1
kqzx2_0_1={'SERVER':'mysql',
      'host':'10.255.202.113',
      'port':3312,
      'user':'echnmarket',
      'passwd':'~+60Y-aAFO5-G]i2:^$:9sg10',
      'db':'echnmarketdb',
      'charset':'utf8',
      }
#卡券中心2.0_2
kqzx2_0_2={'SERVER':'mysql',
      'host':'10.255.202.113',
      'port':3313,
      'user':'echnmarket',
      'passwd':'~+60Y-aAFO5-G]i2:^$:9sg10',
      'db':'echnmarketdb',
      'charset':'utf8',
      }
kaquanzx_test={
      'SERVER':'mysql',
      'host':'10.255.201.198',
      'port':3308,
      'user':'echnmarket2',
      'passwd':'5$^y$]X+94a]K[)q5@Z]gz~n6',
      'db':'echnmarketdb2',
      'charset':'utf8',
      }
db_dict={
      'ecora':ecora,
      'ecora2':ecora2,
      'qrydb':qrydb,
      'usrdb':usrdb,
      'zxkf':zxkf,
      'pjzx':pjzx,
      'kqzx':kqzx,
      'kqzx_test':kqzx_test,
      'kqzx_2_1':kqzx2_0_1,
      'kqzx_2_2':kqzx2_0_2

}
local1={
      'SERVER':'mysql',
      'host':'127.0.0.1',
      'port':3312,
      'user':'echnmarket',
      'passwd':'~+60Y-aAFO5-G]i2:^$:9sg10',
      'db':'',
      'charset':'utf8',


}
local2={
      'SERVER':'mysql',
      'host':'127.0.0.1',
      'port':3313,
      'user':'echnmarket',
      'passwd':'~+60Y-aAFO5-G]i2:^$:9sg10',
      'db':'',
      'charset':'utf8',


}
local_mysql={
      'SERVER': 'mysql',
      'host':'127.0.0.1',
      'port':3306,
      'user':'root',
      'passwd':'250250',
      'db':'',
      'charset':'utf8',
}

jcy={
      'host':'10.255.201.203',
      'port':3308,
      'user':'basedb',
      'passwd':'M$9ETfRuio1i@du6ztz%^!Xd',
      'db':'',
      'charset':'utf8',
}
jcy_local={
      'host':'127.0.0.1',
      'port':3308,
      'user':'basedb',
      'passwd':'M$9ETfRuio1i@du6ztz%^!Xd',
      'db':'',
      'charset':'utf8',
}
jcytest={
      'host':'10.255.201.198',
      'port':3307,
      'user':'basedb',
      'passwd':'M$9ETfRuio1i@du6ztz%^!Xd',
      'db':'',
      'charset':'utf8',
}
kqzx2_qry_3312={'SERVER':'mysql',
      'host': '127.0.0.1',
      #'host':'10.255.202.92',
      'port':33312,
      'user':'echnmarketQry',
      'passwd':'G~KKOn^1tU3AnnASKwvVijMIt',
      'db':'',
      'charset':'utf8',
      }
kqzx2_qry_3313={
      'SERVER':'mysql',
      # 'host':'10.255.202.92',
      'host': '127.0.0.1',
      'port':33313,
      'user':'echnmarketQry',
      'passwd':'G~KKOn^1tU3AnnASKwvVijMIt',
      'db':'',
      'charset':'utf8',
      }
#数据存储目录

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
localdir=os.path.dirname(os.path.abspath(__file__))


data_dir =BASE_DIR+ '/Data'
log_dir =BASE_DIR+ '/Logs'
conf_dir =BASE_DIR+'/conf'
result_dir =BASE_DIR+ '/Result'


if __name__=="__main__":
      print BASE_DIR
      print localdir
      print  log_dir

