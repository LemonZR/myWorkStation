#coding:utf8
import  os,sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
sys.dont_write_bytecode = True
import logging

#log_dir=createPath.mkdir(config.log_dir)

def getLogger(checkDate,logname,logDir):
    log_file = "%s/%s%s.log"%(logDir,logname,checkDate)
    log_level = logging.DEBUG
    logger_name = str(logname)
    logger = logging.getLogger(logger_name)
    fh = logging.FileHandler(log_file)
    #fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    #ch.setLevel(log_level)
    #formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(funcName)s] - [   %(message)s   ]")
    formatter = logging.Formatter("%(asctime)s - %(message)s")
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(log_level)


    return logger


if __name__ == '__main__':
    logger = getLogger('sd','test','./')
    logger.debug("debug message")
    logger.info("info message")
    logger.warn("warn message")
    logger.error("error message")
    logger.critical("critical message")