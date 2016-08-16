# /usr/bin/python
#coding=utf-8

"""
auth:suguoxin
mail:suguoxin@playcrab.com
createtime:2016-04-01 10:58:00
usege:redis简易工具

"""

import redis

class Custom_Redis():

    def __init__(self, using=''):

        """
        @param cursor_hander: 数据库句柄
        """
        self.cursor = None
        self.cursor_hander = using
        self.connections = None
        self.conn = None

        if str(self.cursor_hander).rstrip() == '':
            print 'please write Custom_Redis`s using param'
            exit(0)

        databases = {
            'etl_manage': {'host': 'localhost', 'user': '', 'password': '', 'db': '1', 'charset': 'utf8', 'port': 6379, 'connect_timeout': 50},
            'etl_task': {'host': 'localhost', 'user': '', 'password': '', 'db': '2', 'charset': 'utf8', 'port': 6379, 'connect_timeout': 50},
        }

        database = databases[self.cursor_hander]

        #建立和数据库系统的连接,创建Connection时，指定host及port参数
        self.conn = redis.StrictRedis(host=database['host'], port=database['port'], db=database['db'])


    def keys(self):
        """
        获取所有的key
        :return:
        """
        return self.conn.keys()

    def close(self):
        """
        关闭连接
        :return:
        """
        if self.conn != None:
            del self.conn

    def get(self, key):
        """
        获取一条数据
        :param : key值
        :return: 结果
        """
        return self.conn.get(key)

    def set(self, key, value):
        """
        插入一条数据
        :param : 要插入的数据
        """
        self.conn.set(key, value)

    def delete(self, key):
        """
        删除一条数据
        :param :
        """
        self.conn.delete(key)