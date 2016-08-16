#/usr/bin/python env
#coding=utf-8

"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2014-6-23下午3:45:19
usege:

"""

import sys
import getopt
import platform

'''
版本兼容问题
'''
version = platform.python_version().split('.')[0]
if int(version) == 2:
    import commands
else:
    import subprocess as commands 


class Custom_Command(object):
    
    def __init__(self):
        pass
    
    @staticmethod
    def run(cmd):
        '''
        @param cmd:cmd指令
        return (status,output) 成功状态为0 output为输出 
        '''
        (status,output) = commands.getstatusoutput(cmd)
        return {'status':status,'output':output}

