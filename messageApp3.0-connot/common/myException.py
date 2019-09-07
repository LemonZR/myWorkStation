#!/e3base/python/Python-2.7.6/python
#coding:utf8

import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True


class myException(Exception):
    def __init__(self, exception, *msg):
        self.info = '[%s] %s' % (msg, exception.__str__())

    def __str__(self):
        return self.info


