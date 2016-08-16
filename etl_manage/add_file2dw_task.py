#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2015-10-13 10:00:00
usege: 用于生成etl_data 任务

"""

import datetime,time
from addtask.base import BaseAddTask
from addtask.centerapp import CenterApp

__ALL__=['AddFile2DwTask']
class AddFile2DwTask(BaseAddTask,CenterApp):
    
    def __init__(self):
        BaseAddTask.__init__(self)
        CenterApp.__init__(self)
    
    def make_task_log(self,tasks,log_date,log_time,do_rate='hour'):
        '''
            把需要存储的任务加入列表
        '''
        task_log = []
        #log_date_1 = log_date - datetime.timedelta(days=1) if task['log_name'] == '1day' else log_date

        for task in tasks:
            if task['column_name'] is None:
                continue
            #log_date_1 = log_date if task['log_name'] == 'role_day_info' or task['log_name'] == 'action_log' else log_date - datetime.timedelta(days=1)
            log_date_1 = log_date if task['flag'] == 'snap' or task['log_name'] == 'action_log' else log_date - datetime.timedelta(days=1)

            if do_rate == '1day' or do_rate == '1hour':
                log_info = [task['game'],task['platform'],'',task['log_name'],task['log_dir'],
                            task['md5_name'],task['md5_dir'],task['download_url'],len(task['column_name'].split(",")),
                            task['do_rate'],task['group'],task['priority'],task['from_id'],task['target_id'],task['prefix_sql'],task['load_sql'],task['post_sql'],log_date_1.strftime('%Y%m%d'),log_time,log_date.strftime('%Y%m%d')]
                task_log.append(tuple(log_info))
                
            else:
                task_ip = self.get_platform_ip(task['flag'],task['game'],task['platform'])
                    
                for ip in task_ip:
                    log_info = [task['game'],task['platform'],ip,task['log_name'],task['log_dir'],
                                task['md5_name'],task['md5_dir'],task['download_url'],len(task['column_name'].split(",")),
                                task['do_rate'],task['group'],task['priority'],task['from_id'],task['target_id'],task['prefix_sql'],task['load_sql'],task['post_sql'],log_date.strftime('%Y%m%d'),log_time,log_date.strftime('%Y%m%d')]
                    task_log.append(tuple(log_info))
                
                    if len(task_log) == 1000:
                        self.save_task(task_log)
                        task_log = []
        
        self.save_task(task_log)
        
    def save_task(self,param):
        '''
            存储
        '''
        sql = 'INSERT INTO file2dw_log \
        (`game`,`platform`,`source_ip`,`log_name`,`log_dir`,`md5_name`,`md5_dir`,`download_url`,`col_num`,`do_rate`,`group`,`priority`,`from_id`,`target_id`,`prefix_sql`,`load_sql`,`post_sql`,`log_date`,`log_time`,`task_date`) \
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.mysql.executemany(sql,param) 
        self.mysql.commit()
        
if __name__ == '__main__':
    task = AddFile2DwTask()
    print "start add file2dw task"
    print  time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    
    ip_list = task.get_ip_list()
    #下载任务
    task.get_task('select etl.*,s.column_name,s.flag from etl_data etl left join structure s on etl.target_id = s.id where etl.type = "load" and etl.db_type = "hive" and etl.is_delete = 0',ip_list)
    
    task.close_mysql()
    print "stop add file2dw task"
    print  time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())