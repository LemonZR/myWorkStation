#!/usr/bin/env python
# -*- coding: utf8 -*-

import os
import ConfigParser

"""
# 获取config配置文件
name:配置文件名
1.基本的读取配置文件
    -read(filename) 直接读取ini文件内容
    -sections() 得到所有的section，并以列表的形式返回
    -options(section) 得到该section的所有option
    -items(section) 得到该section的所有键值对
    -get(section,option) 得到section中option的值，返回为string类型
    -getint(section,option) 得到section中option的值，返回为int类型，还有相应的getboolean()和getfloat() 函数。
2.基本的写入配置文件
    -add_section(section) 添加一个新的section
    -set( section, option, value) 对section中的option进行设置，需要调用write将内容写入配置文件。
"""

def getconfig(name,section, key):
    config = ConfigParser.ConfigParser()
    # 获取文件绝对路径
    path = os.path.join(os.path.dirname(__file__).decode('gbk'), name)
    config.read(path)
    return config.get(section, key)
if __name__ == "__main__":
    print getconfig("mail.conf","SITECH_SMTP","login_user")

