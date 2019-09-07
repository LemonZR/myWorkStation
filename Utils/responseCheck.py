#coding:utf8

import os
import time
import json



# 以最合理的编码方式对响应进行解码
def getResponseText(Response):
    try:
        return Response.text
    except Exception as e:
        if type(Response) is str:
            return Response
        else:
            return Response.content


# Requests内置的JSON解码器，可以处理JSON数据
def getResponseJson(Response):
    return Response.json()



# response报文格式校验
def jsonCheck(resp, schema):
    try:
        status = True
        info = resp.get("retMsg")
    except Exception as e:

        status = False
        info = u"报文格式错误"
    return status, info

def resultCheck(Response, code, schema):
    try:
        h_code = Response.status_code
        if h_code == 200:
            resp = getResponseText(Response)
            try:
                # 判断是否需要code是否为“skip”,若是的话，则直接通过，不进行下一步的比较
                if "skip" not in code.split(","):
                    rp = json.loads(resp)
                    rcode = rp["retCode"]
                    rmsg = rp["retMsg"]
                    if rcode in code.split(","):
                        status, info = jsonCheck(resp=rp, schema=schema)
                        result = {"retCode": rcode, "info": info, "state": status, "respone": resp}
                    else:
                        result = {"retCode": rcode, "info": rmsg, "state": False, "respone": resp}
                else:
                    result = {"retCode": "skip", "info": u"该用例不进行结果校验", "state": "True", "respone": ""}
            except Exception as e:

                result = {"retCode": "800001", "info": u"响应报文解析异常", "state": False, "respone": resp}
        else:
            result = {"retCode": "800002", "info": u"status code error %s"% h_code, "state": False, "respone": ""}
    except Exception as e:

        if Response == "ConnectTimeout":
            result = {"retCode": "800003", "info": u"与服务器连接超时【ConnectTimeout】", "state": False, "respone": ""}
        elif Response == "Timeout":
            result = {"retCode": "800004", "info": u"响应超时【Timeout】", "state": False, "respone": ""}
        elif Response == "ConnectionError":
            result = {"retCode": "800005", "info": u"网络受限【ConnectionError】", "state": False, "respone": ""}
        else:
            result = {"retCode": "800006", "info": u"系统异常【Response】", "state": False, "respone": ""}
    return result



