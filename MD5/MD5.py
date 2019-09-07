#coding:utf8

import hashlib

def md5_data(str,fixx='sitech2019'):
    #satl是盐值，默认是123456
    str=str+fixx
    md = hashlib.md5()  # 构造一个md5对象
    md.update(str.encode())
    res = md.hexdigest()
    print fixx
    return res

# s='jmy123'
# new_s='jmy123问'.encode() #字符串变成byetes类型
# print('encode....',new_s)
# print('decode',new_s.decode()) #
if __name__=='__main__':

    asd=md5_data(" ")
    print asd