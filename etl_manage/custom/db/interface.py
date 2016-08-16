#coding=utf-8



from abc import ABCMeta  
from abc import abstractproperty
from abc import abstractmethod 


class Custom_Interface(object):
    
    __metaclass__ = ABCMeta



    @abstractmethod
    def close(self):
        """
        关闭当前数据库句柄
        """
        return
    @abstractmethod
    def query(self, query, *parameters):
        """
        @return: 返回一个list，多个结果。
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        return
       
    @abstractmethod
    def get(self, query, *parameters):
        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        return
       
    @abstractmethod
    def count(self,query, *parameters):
        """
        @return: 返回单个结果
        @param query:SQL语句
        @param parameters:SQL语句参数
        """
        return
       
    @abstractmethod
    def insert(self,table,**datas):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return
       
    @abstractmethod
    def update(self,table,where,**datas):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return
        
    @abstractmethod
    def delete(self,table,where):
        '''
        @param table:表名
        @param datas:｛字段：值｝
        '''
        return
  