#/usr/bin/python
#coding=utf-8

"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2014-6-26下午6:03:31
usege:

"""

import paramiko
import traceback


def trace_back():
    try:
        return traceback.format_exc()
    except:
        return ''
    
__all__ = ['Custom_sftp']

#设置日志记录
paramiko.util.log_to_file('/tmp/test')

class Custom_sftp(object):
    
    def __init__(self,**host):
        self.host = host
        self.error = ''
        
    def run(self,src,des,type='get'):
        
        try:
            
            #建立一个加密的管道
            self.scp=paramiko.Transport((self.host['ip'],self.host['port']))
            
            #建立连接
            import os
            host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
            self.scp.connect(username=self.host['user'],password=self.host['pwd'], pkey=host_keys)
            
            #建立一个sftp客户端对象，通过ssh transport操作远程文件
            self.sftp=paramiko.SFTPClient.from_transport(self.scp)
            
            if type == 'get':
    
                self.sftp.get(src,des)
            
            if type == 'put':
                self.sftp.put(src,des)
            
        
            self.scp.close()
            self.sftp.close()   
            

            
            return {'flag':'1','ip':self.host['ip'],'data':'successful'}
 
        except:
            return {'flag':'0','ip':self.host['ip'],'data':trace_back()}
            exit()
            

        
    def __del__(self):
        pass
