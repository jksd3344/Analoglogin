#/usr/bin/python
#coding=utf-8

'''
auth:wuqichao@playcrab.com
date:2014-06-17 12:00

'''
import time
import datetime
import json
import os,sys
import signal
from pprint import pprint
from multiprocessing import Process,active_children,JoinableQueue


from custom.log.custom_log import w_info
from custom.db.mysql import Custom_MySQL
from custom.command.remote.cmd import Custom_cmd
from custom.command.remote.ssh import Custom_ssh
from custom.command.remote.sftp import Custom_sftp
from custom.command.remote.scp import Custom_scp


__all__ = ['Custom_Process_ssh','Custom_Process_cmd','Custom_Process_sftp']

class Custom_Process_cmd(Process):
    
    grandchild = JoinableQueue()
    
    def __init__(self,child_conn,cmd,**host): 
        Process.__init__(self)
        self.host = host
        self.cmd  = cmd
        self.child_conn = child_conn
        self.start_time = datetime.datetime.now()
        self.child_timeout = 40
        self.child_process = None
        
        
     
    def kill_child(self):

        try:
            self.child_process.terminate()
        except:
            pass

        try:
            os.kill(self.child_process.pid,signal.SIGKILL)
        except:
            pass
        
    def __kill_current_process(self):
        '''当前进程自杀'''

      
        try:
            self.terminate()
        except:
            pass
        try:
            os.kill(self.pid(), signal.SIGKILL)
            os._exit(1)
        except:
            pass
                          
    def run(self):
        
        
        self.child_process = Custom_ssh(Custom_Process_cmd.grandchild,self.cmd,**self.host)
        self.child_process.start()
        self.child_process.join(timeout = self.child_timeout)
       
        try:   
            result = Custom_Process_cmd.grandchild.get(timeout=3)
            print json.dumps(self.host['ip'])
            self.child_conn.put(result)
            self.kill_child()
        except:
            print json.dumps(self.host['ip'])
            self.child_conn.put({'flag':0,'ip':self.host['ip']})
            self.kill_child()
            
        self.__kill_current_process()   
        
 

                


        
class Custom_Process_ssh(Process):
    
    def __init__(self,cmd,**host): 
        Process.__init__(self)
        self.host = host
        self.cmd  = cmd
    
    def run(self):
        
        ssh = Custom_ssh(**self.host)
        result = ssh.run(self.cmd)
        pprint(result)
        path = 'log'
        try:
            path = self.cmd.split()[0].replace('/','-')
        except:
            path = self.cmd
            
        w_info("===============start===================",path)
        w_info(result['ip'],path)
        w_info(result['data'],path)
        w_info("===============end===================",path)
    
class Custom_Process_sftp(Process):
    
    def __init__(self,src,des,**host): 
        Process.__init__(self)
        self.host = host
        self.src  = src
        self.des  = des
    
    def run(self):
    
        ssh = Custom_sftp(**self.host)
        ssh.run(self.src,self.des)
