#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2015-10-13 10:00:00
usege: 用于生成etl_data 任务

"""

import datetime,time
from custom_online.db.mysql import Custom_MySQL
import re

__ALL__=['BaseAddReportTask']
class BaseAddReportTask():
    def __init__(self,start_date = None,end_date=None):
        self.mysql = Custom_MySQL(using='etl_manage')
        
        self.task_date = datetime.datetime.now().strftime("%Y%m%d")
        
        yestoday = datetime.datetime.now() - datetime.timedelta(days=1)
        if start_date is None:
            self.start_date = yestoday
        else:
            self.start_date = datetime.datetime.strptime(start_date,"%Y%m%d") - datetime.timedelta(days=1)
        
        if end_date is None:
            self.end_date = yestoday
        else:
            self.end_date = datetime.datetime.strptime(end_date,"%Y%m%d") - datetime.timedelta(days=1)
        
        
    def get_task(self,sql):
        '''
            获取执行任务
        '''
        result=True
        try:
            tasks  = self.mysql.query(sql)
            if len(tasks)==0:
                result=False
            else:
                one_day_list = []
                seven_day_list = []
                thirty_day_list = []
                for task in tasks:
                    if task['do_rate'].encode('utf8') == '1day':
                        one_day_list.append(task)
                    if task['do_rate'].encode('utf8') == '7day':
                        seven_day_list.append(task)
                    if task['do_rate'].encode('utf8') == '30day':
                        thirty_day_list.append(task)


                #日任务
                if one_day_list:
                    self.create_task_log(one_day_list,'day')

                #周任务
                if seven_day_list:
                    self.create_task_log(seven_day_list,'week')

                #月任务
                if thirty_day_list:
                    self.create_task_log(thirty_day_list,'month')

        except Exception as exc:
            print exc
            result=False
            #异常回滚
            self.mysql.rollback()
        finally:
            return result
    def create_task_log(self,tasks,flag = 'day'):
        '''
            创建执行任务
        '''
        task_log = []
        
        i = 0;
        do_date = self.start_date
        
        while do_date <= self.end_date:
            i = i + 1
            #判断是否是周一
            if flag == 'week' and do_date.weekday() != 6:
                do_date = self.start_date + datetime.timedelta(days = i)
                continue
            
            #判断是否是1号
            t_today = do_date + datetime.timedelta(days=1)
            if flag == 'month' and t_today.strftime("%d") != '01':
                do_date = self.start_date + datetime.timedelta(days = i)
                continue
            
            #加入列表
            task_log = self.make_task_log(tasks,task_log,do_date.strftime("%Y%m%d"),self.task_date)
            do_date = self.start_date + datetime.timedelta(days = i)
            
            
        #最后存储
        self.save_task(task_log)
    
   
    def clear_params(self,task,log_date):
        '''
            替换所有的临时参数
        '''
        platform_str = ''
        platform_str_partion=''
        if task['platform'] != 'all':
            platform_str = "AND platform = '%s'" % task['platform']
            platform_str_partion="AND plat_form = '%s'" % task['platform']
        
        log_time = datetime.datetime.strptime(log_date,"%Y%m%d").strftime("%Y-%m-%d")
        
        replace_prefix_sql = ""
        replace_exec_sql = ""
        if task['prefix_sql'] is not None:
            replace_prefix_sql = task['prefix_sql'].replace("{db_name}",task['db_name']).replace("{table_name}",task['table_name']) \
                                    .replace("{log_date}",log_time).replace("{platform}",platform_str)
        if task['exec_sql'] is not None:
            replace_exec_sql = task['exec_sql'].replace("{log_date}",log_time).replace("{platform}",platform_str_partion)
        if self.time_zone is not None:
            replace_exec_sql = replace_exec_sql.replace("{time_zone}",self.time_zone)
        task['replace_prefix_sql'] = replace_prefix_sql
        task['replace_exec_sql'] = replace_exec_sql
        
        return task
    
    def close_mysql(self):
        '''
            关闭数据库
        '''
        self.mysql.close();