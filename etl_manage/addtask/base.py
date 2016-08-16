#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2015-10-13 10:00:00
usege: 用于生成etl_data 任务

"""

import datetime
from custom_online.db.mysql import Custom_MySQL

__ALL__=['BaseAddTask']
class BaseAddTask():
    def __init__(self,start_time = None,end_time = None,is_action = None):
        if is_action==None:
            self.mysql = Custom_MySQL(using='etl_manage')
        else:
            self.mysql = Custom_MySQL(using='kok_action')

        if start_time == None:
            tomorrow = datetime.datetime.now() + datetime.timedelta(days=1)
            self.start_time = datetime.datetime.strptime(tomorrow.strftime("%Y-%m-%d 00:00:00"),"%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime(start_time,"%Y-%m-%d %H:%M:%S")
        
        
        if end_time == None:  
            after_tomorrow = datetime.datetime.now() + datetime.timedelta(days=2)
            self.end_time = datetime.datetime.strptime(after_tomorrow.strftime("%Y-%m-%d 00:00:00"),"%Y-%m-%d %H:%M:%S")
        else:
            self.end_time = datetime.datetime.strptime(end_time,"%Y-%m-%d %H:%M:%S")
        
        
        print self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        print self.end_time.strftime("%Y-%m-%d %H:%M:%S")
        
        
    def get_task(self,sql,ip_list):
        '''
            获取执行任务
        '''
        try:
            
            self.ip_list = ip_list
            tasks  = self.mysql.query(sql)
            
            five_min_list = []
            one_day_list = []
            one_hour_list = []
            for task in tasks:
                if task['do_rate'].encode('utf8') == '5min':
                    five_min_list.append(task)
                if task['do_rate'].encode('utf8') == '1day':
                    one_day_list.append(task)
                if task['do_rate'].encode('utf8') == '1hour':
                    one_hour_list.append(task)

            #五分钟任务
            if five_min_list:
                self.create_fivemin_task_log(five_min_list)
            
            #1小时任务
            if one_hour_list:
                self.create_onehour_task_log(one_hour_list) 
            
            #一天任务
            if one_day_list:
                self.create_oneday_task_log(one_day_list) 
                    
        except Exception as exc:
            print exc
            #异常回滚
            self.mysql.rollback()
    
    def create_oneday_task_log(self,tasks):
        '''
            创建1天执行任务
        '''
        i = 1;
        do_time = self.start_time
        
        while do_time < self.end_time:
            log_date = do_time
            log_time = '0000'
            
            i = i + 1
            do_time = self.start_time + datetime.timedelta(days = i)
        
            #加入列表
            self.make_task_log(tasks,log_date,log_time,do_rate='1day')
            
        #最后存储
        #self.save_task(task_log)
    
    def create_onehour_task_log(self,tasks):
        '''
            创建1小时执行任务
        '''
        i = 1;
        do_time = self.start_time
        
        
        while do_time < self.end_time:
            do_time = self.start_time + datetime.timedelta(hours = i)
            log_date = do_time
            
            log_time = do_time.strftime("%H%M")
            if log_time == '0000':
                log_time = '2400'
                log_date = do_time - datetime.timedelta(days = 1)
            
            i = i + 1
        
            #加入列表
            task_log = self.make_task_log(tasks,log_date,log_time,do_rate='1hour')
           
        #最后存储
        #self.save_task(task_log)
    
    def create_fivemin_task_log(self,tasks):
        '''
            创建5分钟执行任务
        '''
        i = 1;
        do_time = self.start_time
        
        while do_time < self.end_time:
            do_time = self.start_time + datetime.timedelta(minutes = i*5)
            log_date = do_time
            
            log_time = do_time.strftime("%H%M")
            if log_time == '0000':
                log_time = '2400'
                log_date = do_time - datetime.timedelta(days = 1)
                
            i = i + 1
            
            #加入列表
            task_log = self.make_task_log(tasks,log_date,log_time,do_rate='minutes')

        #最后存储
        #self.save_task(task_log)
    def get_platform_ip(self,flag,game,platform):
        '''
            从三维数组中获取IP
        '''
        # task_ip = self.ip_list[flag]
        # if flag in ['log','snap']:
        #     if game not in self.ip_list[flag] or platform not in self.ip_list[flag][game]:
        #         return []
        #     task_ip = self.ip_list[flag][game][platform]
        if game in ['basic', 'mostsdk', 'wanpay']:
            task_ip = self.ip_list[game]
        else:
            if game not in self.ip_list[flag] or platform not in self.ip_list[flag][game]:
                return []
            task_ip = self.ip_list[flag][game][platform]
            
        return task_ip
    def close_mysql(self):
        '''
            关闭数据库
        '''
        self.mysql.close();