#coding:utf8
import os

def mkdir(path):

    import os

    path = path.strip()
    path = path.rstrip("\\")


    isPathExists = os.path.exists(path)

    # 判断结果
    if not isPathExists:
        # 如果不存在则创建目录
        os.makedirs(path)
        print path + ' 创建成功'
        return path
    else:
        return path


# 定义要创建的目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
localdir = os.path.dirname(os.path.abspath(__file__))
dirs={
    'logDir':'/ZR/SendCard/log',
    'dataDir':'/ZR/SendCard/data',
    'confDir':'/ZR/SendCard/conf',
    'commonDir' : '/ZR/SendCard/commonality',
    'utilsDir': '/ZR/SendCard/Utils',
    'result_dir':'/ZR/SendCard/result'
}

# 调用函数
if __name__=='__main__':
    import sys

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(BASE_DIR)
    from conf import config
    mkdir(config.data_dir+"aa")