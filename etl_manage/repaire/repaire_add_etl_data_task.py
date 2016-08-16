#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2015-10-13 10:00:00
usege: 用于生成etl_data 任务

"""

import sys
sys.path.append("./")
import datetime,time
from addtask.base import BaseAddTask
from addtask.centerapp import CenterApp
import optparse

__ALL__=['AddEtlDataTask']
class AddEtlDataTask(BaseAddTask,CenterApp):
    
    def __init__(self,start_time = None,end_time = None):
        BaseAddTask.__init__(self,start_time,end_time)
        CenterApp.__init__(self)
    
    def make_task_log(self,tasks,log_date,log_time,do_rate = 'hour'):
        '''
            把需要存储的任务加入列表
        '''
        task_log = []
        for task in tasks:
            if task['column_name'] is None:
                continue
            task_ip = self.get_platform_ip(task['flag'],task['game'],task['platform'])
            for ip in task_ip:
                log_info = [task['game'],task['platform'],ip,task['log_name'],task['log_dir'],
                            task['md5_name'],task['md5_dir'],task['download_url'],len(task['column_name'].split(",")),
                            task['do_rate'],task['group'],task['priority'],task['from_id'],task['target_id'],log_date.strftime('%Y%m%d'),log_time,log_date.strftime('%Y%m%d')]
                task_log.append(tuple(log_info))
            
                if len(task_log) == 1000:
                    self.save_task(task_log)
                    task_log = []
        
        self.save_task(task_log)
        
    def save_task(self,param):
        '''
            存储
        '''
        sql = 'INSERT INTO etl_data_log \
        (`game`,`platform`,`source_ip`,`log_name`,`log_dir`,`md5_name`,`md5_dir`,`download_url`,`col_num`,`do_rate`,`group`,`priority`,`from_id`,`target_id`,`log_date`,`log_time`,`task_date`) \
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.mysql.executemany(sql,param) 
        self.mysql.commit()
        
if __name__ == '__main__':
    
    parser = optparse.OptionParser()
    parser.add_option(
        '--game',
        dest='game',
        help='game name of task',
        metavar='GAME',
    )
    parser.add_option(
        '--platform',
        dest='platform',
        help='platform name of task',
        metavar='PLATFORM',
    )
    parser.add_option(
        '--start_time',
        dest='start_time',
        help='start time of task,eg:2016-01-01 00:00:00',
        metavar='START_TIME',
    )
    parser.add_option(
        '--end_time',
        dest='end_time',
        help='end time of task,eg:2016-01-01 00:00:00',
        metavar='END_TIME',
    )
    options, args = parser.parse_args()

    task = AddEtlDataTask(options.start_time,options.end_time)
    
    print "start add etl_data task"
    print  time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    #下载任务
    
    condition = '1'
    if options.game is not None:
        condition += ' and etl.game = "%s"' % options.game
    if options.platform is not None:
        condition += ' and etl.platform = "%s"' % options.platform
    
    ip_list = task.get_ip_list()
    
    task.get_task('select etl.*,s.column_name,s.flag from etl_data etl left join structure s on etl.target_id = s.id where %s and etl.type = "download" and etl.is_delete=0 ' % condition,ip_list)
    
    task.close_mysql()
    
    print "stop add etl_data task"
    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    
    
    
    
    
