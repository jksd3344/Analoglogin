#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2016-01-11 10:00:00
usege: 创建dm2report任务

"""

import datetime,time
from custom.db.mysql import Custom_MySQL
import optparse

__ALL__=['CopyConfig']
class CopyConfig():
    
    def __init__(self):
        self.mysql = Custom_MySQL(using='etl_manage')
        self.source_game = 'ares'
    
    def get_all_task(self,task_name):
        
        condition = 'game = "%s" ' % self.source_game
        if task_name is not None:
            condition += 'and task_name="%s"' % task_name
        
        task_list = self.mysql.query("select * from dm2report where is_delete = 0 and %s" % condition)
        return task_list
    def get_structure(self,id,game):
        structure=self.mysql.get("select * from structure where is_delete=0 and id=%s",id)
        if structure!=None:
            t_structure=[
                structure['type'],
                structure['flag'],
                structure['db_type'],
                game,
                structure['platform'],
                #'db_name':structure['db_name'],
                structure['table_name'],
                structure['column_name'],
                ##structure['partition_name'],
                ##structure['partition_rule'],
                ##structure['index_name'],
                structure['create_table_sql'],
                structure['user_id'],
                0,
                datetime.datetime.today().strftime("%Y-%m-%d")
            ]
            game_db=None
            if structure['db_type']!=None and str(structure['db_type']).__eq__('hive'):
                game_db='%s_dw' % game
                t_structure.append(game_db)
            elif structure['db_type']!=None and str(structure['db_type']).__eq__('mysql'):
                game_db='report_%s' % game
                t_structure.append(game_db)
            exis_row=self.mysql.query("select id from structure where platform='all' and user_id='wxx' and is_delete=0 and db_name='%s' and table_name='%s' and db_type='%s'"%(game_db,str(structure['table_name']),str(structure['db_type'])))
            if len(exis_row)>0:
                return  int(exis_row[0]['id'])
            else:
                return self.save_newstructure(t_structure)


    def save_new_task(self,task):
        self.mysql.insert("dm2report",**task)
        self.mysql.commit()
    def save_newstructure(self,structure):
        query='INSERT INTO structure(type,flag,db_type,game,platform,table_name,column_name,create_table_sql,user_id,is_delete,create_date,db_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        rowNum=self.mysql.execute(query,*tuple(structure))
        self.mysql.commit()
        return rowNum
    def run(self,game,task_name=None):
        print "start copy"
        task_list = self.get_all_task(task_name)
        
        for task in task_list:
            form_id=self.get_structure(int(task['from_id']),game)
            target_id=self.get_structure(int(task['target_id']),game)
            t_task = {
                'game':game,
                'platform':task['platform'],
                'task_name':task['task_name'],
                'date_cycle':task['date_cycle'],
                'do_rate':task['do_rate'],
                'group':task['group'],
                'priority':task['priority'],
                'prefix_sql':task['prefix_sql'],
                'exec_sql':task['exec_sql'].replace("%s_dw" % self.source_game,"%s_dw" % game),
                'post_sql':task['post_sql'],
                'from_id':form_id,
                'target_id':target_id,
                'create_date':datetime.datetime.today().strftime("%Y-%m-%d"),
                'comment':task['comment']
            }

            self.save_new_task(t_task)
        
        self.mysql.close()
        print "over"
    
if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option(
        '--game',
        dest='game',
        help='game name of task',
        metavar='GAME',
    )
    parser.add_option(
        '--taskname',
        dest='taskname',
        help='task_name',
        metavar='TASKNAME',
    )
    options, args = parser.parse_args()
    ##options.game='hebe'
    task = CopyConfig()
    
    if options.game:
        task.run(
            game = options.game,
            task_name = options.taskname
        )
    else:
        parser.print_help()
