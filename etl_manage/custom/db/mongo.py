#/usr/bin/python
#coding=utf-8

"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2014-6-26下午12:13:07
usege:

"""

import sys


try:
    import pymongo

except ImportError:
    print >> sys.stderr,"""\

There was a problem importing Python modules(pymongo) required.
The error leading to this problem was:
%s Please install a package which provides this module, or
verify that the module is installed correctly.
you can use this cmd :pip install pymongo

It's possible that the above module doesn't match the current version of Python,
which is:
%s
""" % (sys.exc_info(), sys.version)

from interface import Custom_Interface

class Custom_Mongo(Custom_Interface):
    
    def __init__(self,using=''):

        """
        @param cursor_hander:数据库句柄
        """
        self.cursor = None
        self.cursor_hander = using
        self.connections = None
        self.conn  = None
        
        if str(self.cursor_hander).rstrip() == '':
            print 'please write Custom_MySQL`s using param'
            exit(0)
   
            

        databases ={
            'logs':{'host':'127.0.0.1', 'user':'readonly','password':'readonly', 'database':'local','charset':'utf8','port':27017,'connect_timeout':50},
            
            }
        
        database = databases[self.cursor_hander]
        
        #建立和数据库系统的连接,创建Connection时，指定host及port参数
        self.conn   = pymongo.Connection(host=database['host'],port=database['port'])
        self.database = self.conn[database['database']]
        #admin 数据库有帐号，连接-认证-切换库
        #db_auth = conn.admin
        #db_auth.authenticate('sa','sa')

        
    def __del__(self):
        
        self.close()
        
     
    def close(self):
        """
        关闭当前数据库句柄
        """
        if self.conn != None:
            return self.conn.disconnect()
     
    def query(self, table_name, parameters,skip=None,limit=None):
        """
        @return: 返回一个list，多个结果。
        @param table_name:表名
        @param parameters:SQL语句参数
        """
        result = self.database[table_name].find(parameters)
        
        if skip !=None:
            result.skip(skip)
        if limit !=None:
                
            result.limit(limit) 
            
        return result
       
     
    def get(self, table_name,**parameters):
        """
        @return: 返回单个结果
        @param table_name:表名
        @param parameters:SQL语句参数
        """
        return self.database[table_name].find_one(parameters)
       
     
    def count(self,table_name, **parameters):
        """
        @return: 返回单个结果
        @param table_name:表名
        @param parameters:SQL语句参数
        """
        return self.database[table_name].find(parameters).count()
       
     
    def insert(self,table,**datas):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return self.database[table].insert(datas)
       
     
    def update(self,table,where,**datas):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return self.database[table].update(where,datas)
        
     
    def delete(self,table,where):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return self.database[table].remove(where)



