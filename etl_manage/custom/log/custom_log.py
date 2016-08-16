#!/usr/bin/python
#coding:utf-8
"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2014-7-1下午12:24:29
usege:

"""

import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='myapp.log',
                filemode='w')

#################################################################################################
#定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
#################################################################################################

logging.debug('This is debug message')
logging.info('This is info message')
logging.warning('This is warning message')




    
import logging
from mongolog.handlers import MongoHandler
log = logging.getLogger('demo')
log.setLevel(logging.DEBUG)
log.addHandler(MongoHandler.to(db='mongolog', collection='log'))




log.debug("1 - debug message")
log.info("2 - info message")
log.warn("3 - warn message")
log.error("4 - error message")
log.critical("5 - critical message")