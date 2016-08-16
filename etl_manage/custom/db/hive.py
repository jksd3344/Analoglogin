#coding=utf-8
'''
Created on 2015-9-28
@author: wuqichao
@copyright: $Id: custom_hive.py 12565 2013-01-29 08:00:34Z wuqichao $
'''
import sys
import platform
'''
版本兼容问题
'''
version = platform.python_version().split('.')[0]
if int(version) == 2:
    import commands
else:
    import subprocess as commands 

try:
    import pyhs2

except ImportError:
    print >> sys.stderr,"""\

There was a problem importing Python modules(pyhs2) required.
The error leading to this problem was:
%s Please install a package which provides this module, or
verify that the module is installed correctly.

It's possible that the above module doesn't match the current version of Python,
which is:
%s
""" % (sys.exc_info(), sys.version)



__ALL__=['Custom_Hive']

class Custom_Hive() :
    '''
    service 基础类
    '''

    def __init__(self,using=''):
        """
        @param cursor_hander:数据库句柄
        """
        self.cursor = None
        self.cursor_hander = using
        self.connections = None
        
        if str(self.cursor_hander).rstrip() == '':
            print('please write Custom_Hive`s using param')
            exit(0)

                        
        databases = {
            'ares_dw':{'host':'10.0.0.2', 'user':'hive', 'password':'', 'database':'test', 'port':10000 ,'authMechanism':'NOSASL'},
            'hadoops2':{'host':'10.0.0.2', 'user':'hive', 'password':'', 'database':'test', 'port':10000 ,'authMechanism':'NOSASL'},
		    'basic_data':{'host':'10.0.0.2', 'user':'hive', 'password':'', 'database':'basic_data', 'port':10000 ,'authMechanism':'NOSASL'}
        }
        
        database = databases[self.cursor_hander]
	
	self.connections= pyhs2.connect(host=database['host'],
                      port= int(database['port']),
                      authMechanism= database['authMechanism'],
                      user=database['user'],
                      password=database['password'],
                      database=database['database'],
                      )

        self.cursor = self.connections.cursor()

    def close(self):
        try:
            if self.connections:
                self.connections.close()
        except:
            pass

    def __del__(self):
        try:
            if self.connections:
                self.connections.close()
        except:
            pass

            
    def _execute(self, cursor, query):

 
        return cursor.execute(query)



    def query(self, query, *parameters):
        """
        @return: 返回一个list，多个结果。
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        try:
            self._execute(self.cursor,query)
            return self.cursor.fetch()

        except :
            Exception("OperationalError for Custom_Hive.query() query")
            raise

    def get(self, query, *parameters):
        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """

        try:
            self._execute(self.cursor,query)
            return self.cursor.fetchone()

        except :
            Exception("OperationalError for Custom_Hive.get() query")
            raise

    def count(self,query, *parameters):
        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        try:
            self._execute(self.cursor,query)
            return self.cursor.fetchone()

        except :
            Exception("OperationalError for Custom_Hive.count() query")
            raise

    def load(self,sql):

        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        if sql.lower().find('load') == -1:
            return {'status':'-1','output':'This is error hive SQL'}

        cmd_line = '''hive -S -e "%s;"'''% sql

        (status,output) = commands.getstatusoutput(cmd_line)
        return {'status':status,'output':output}

		
    def move(self,sql):


        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """

        if sql.lower().find('move') == -1:
            return {'status':'-1','output':'This is error hive SQL'}

        cmd_line = '''hive -S -e "%s;"'''% sql
        
        (status,output) = commands.getstatusoutput(cmd_line)
        return {'status':status,'output':output}

    def dump(self, sql, path):


        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """

        #if sql.lower().find('dump') == -1:
        #    return {'sql':sql,'status':'-1','output':'This is error hive SQL'}

        cmd_line = '''hive -S -e "%s;" > %s'''% (sql,path)
        #cmd_line = '''/opt/cloudera/parcels/CDH/bin/hive -S -e "%s;" > %s''' % (sql, path)

        (status, output) = commands.getstatusoutput(cmd_line)
        return {'status': status, 'output': output}

    def select_insert(self,sql):


        """
        @return: 执行select xxx insert xxx
        @param sql:SQL语句
        """

        if sql.lower().find('insert') == -1:
            return {'sql':sql,'status':'-1','output':'This is error hive SQL'}

        cmd_line = '''hive -S -e "%s;"'''% (sql)

        (status,output) = commands.getstatusoutput(cmd_line)
        return {'status':status,'output':output}