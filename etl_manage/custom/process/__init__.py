#!/usr/bin/env python
#coding=gbk

###################################################
#  日期：2013-05-30
#  作者：吴启超
#  功能：采集糗百笑话，利用进程池,更改了一下系统pool的默认行为支持daemon模式

#

import multiprocessing
# 我们必须显示声明引进multiprocessing模块，而不是Process 

import multiprocessing.pool
import time

from random import randint
from multiprocessing  import Process

class TaskProcess(Process):

    def __init__(self):
        Process.__init__(self)

    def __del__(self):
        pass 

    def run(self):
        print 'TaskProcess is running '

class NoDaemonProcess(multiprocessing.Process):
    # 使进程总是daemon模式
   def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon,None,doc='make "daemon" attribute always return False')

# 我们用multiprocessing.pool.Pool 代替 multiprocessing.Pool
# 因为这里只有最新的包装器函数, 而不是一个类.
class CustomPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess



def work(num_procs):
    print("Creating %i (daemon) workers and jobs in child." % num_procs)
    #自己的业务进程
   p = TaskProcess()
    p.start()

def test():
    print("Creating 5 (non-daemon) workers and jobs in main process.")

    year = [x for x in range(2008, 2014)]

    pool = CustomPool(len(year)*4)

    result = pool.map(work,year)

    pool.close()
    pool.join()
    #print(result)

# if __name__ == '__main__':
#     test()
