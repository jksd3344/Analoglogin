#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2016-01-11 10:00:00
usege: 创建dm2report任务

"""
import sys
import datetime,time
from addtask.basereport import BaseAddReportTask
import optparse

__ALL__=['AddDm2ReportTaskNew']
class AddDm2ReportTaskNew(BaseAddReportTask):
    
    def __init__(self,start_date,end_date,platform = None,do_rate = None,time_zone=None):
        self.platform = platform if platform is not None else "all"
        self.do_rate = do_rate
        self.time_zone=time_zone if time_zone is not None else ""
        BaseAddReportTask.__init__(self,start_date,end_date)
    
    def make_task_log(self,tasks,task_log,log_date,task_date):
        '''
            把需要存储的任务加入列表
        '''
        for task in tasks: 
            task['platform'] = self.platform
            task = self.clear_params(task,log_date)
            form_id=None
            target_id=None
            if task['platform'] != 'all':
                form_id=self.replace_sourceid(task['from_id'],task['game'],task['platform'])
                target_id=self.replace_sourceid(task['target_id'],task['game'],task['platform'])
            else:
                form_id= task['from_id']
                target_id=task['target_id']

            log_info = [task['game'],task['platform'],task['task_name'],task['date_cycle'],task['do_rate'],
                        task['grouped'],task['priority'],task['replace_prefix_sql'],task['replace_exec_sql'],task['post_sql'],
                        form_id,target_id,log_date,task_date,task['comment']]
            task_log.append(tuple(log_info))
            if len(task_log) == 1000:
                self.save_task(task_log)
                task_log = []
        
        return task_log

    def replace_sourceid(self,id,game,platform):
        '''
        :param id:已知的id
        :param type:逻辑类型(source,dw,dm,report)
        :param game:游戏名称
        :param platform:渠道名称
        :return:
        '''
        structures=self.mysql.query("SELECT id,db_type,type,table_name FROM structure WHERE id IN (%s)"%(id))
        structure_ids=""
        for structure in structures:
            rs=self.mysql.get("SELECT id FROM structure WHERE game='%s' AND platform='%s' AND db_type='%s' AND table_name='%s' AND type='%s'"%(game,platform,structure['db_type'],structure['table_name'],structure['type']))
            id=str(rs["id"])
            if len(str(structure_ids))>0:
                structure_ids=structure_ids+","+id
            else:
                structure_ids=id
        return structure_ids

    def save_task(self,param):
        '''
            存储
        '''
        sql = 'INSERT INTO dm2report_new_log \
        (`game`,`platform`,`task_name`,`date_cycle`,`do_rate`,`grouped`,`priority`,`prefix_sql`,`exec_sql`,`post_sql`,`from_id`,`target_id`,`log_date`,`task_date`,`comment`) \
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.mysql.executemany(sql,param) 
        self.mysql.commit()
    
    def run(self,game = None,platform = "all",task_name = None,cycle = None):
        '''
            执行
        '''
        print "start add dm2report task"
        
        #condition = ''
        condition='and a.platform = "all" '
        if game is not None:
            condition += 'and a.game = "%s" ' % game

        if task_name is not None:
            condition += 'and a.task_name = "%s" ' % task_name

        if cycle is not None:
            condition += 'and a.date_cycle = "%s" ' % cycle
        
        if self.do_rate is not None:
            condition += 'and a.do_rate = "%s" ' % self.do_rate
        
        result=self.get_task('select a.*,s.db_name,s.table_name from dm2report_new a left join structure s on a.target_id = s.id where  a.is_delete = 0 %s' % condition)
        print "stop add dm2report task"
        print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    def clean(self):
        self.close_mysql()
        
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
        '--taskname',
        dest='taskname',
        help='task_name',
        metavar='TASKNAME',
    )
    parser.add_option(
        '--cycle',
        dest='cycle',
        help='date_cycle',
        metavar='CYCLE',
        type='int',
    )
    parser.add_option(
        '--do_rate',
        dest='do_rate',
        help='do_rate name of task',
        metavar='DO_RATE',
    )
    parser.add_option(
        '--time_zone',
        dest='time_zone',
        help='platform time zone set.1hs like 3600(second)',
        metavar='TIME_ZONE',
    )
    parser.add_option(
        '--start_date',
        dest='start_date',
        help='start date of task,eg:20160101',
        metavar='START_DATE',
    )
    parser.add_option(
        '--end_date',
        dest='end_date',
        help='end date of task,eg:20160101',
        metavar='END_DATE',
    )
    options, args = parser.parse_args()

    task = AddDm2ReportTaskNew(options.start_date,options.end_date,options.platform,options.do_rate,options.time_zone)

    if options.game or options.taskname or options.cycle:
        task.run(
            game = options.game,
            platform= options.platform if options.platform is not None else "all",
            task_name = options.taskname,
            cycle = options.cycle
        )
    else:
        task.run()
    task.clean()
#         parser.print_help()
#     print options
#     print args
