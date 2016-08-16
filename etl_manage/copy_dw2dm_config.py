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

__ALL__=['COPY_DW2DM_CONFIG']
##log_name
class CopyConfig():
    
    def __init__(self):
        self.mysql = Custom_MySQL(using='etl_manage')
        self.source_game = 'ares'
    
    def get_all_task(self,task_name):
        
        condition = 'game = "%s" ' % self.source_game
        if task_name is not None:
            condition += 'and task_name="%s"' % task_name
        ##appstoremix is_delete = 0 and
        task_list = self.mysql.query("select * from dw2dm where  platform='all' and %s" % (condition))
        return task_list

    def get_structure(self,id,game,plat_form):
        '''
         获取当前游戏的，参数structure.如不存在则会添加
        :param id:
        :param game:
        :param plat_form:
        :return:
        '''
        structure=self.mysql.get("select * from structure where is_delete=0 and id=%s",id)
        if structure!=None:
            t_structure=[
                structure['type'],
                structure['flag'],
                structure['db_type'],
                game,
                plat_form,
                #structure['platform'],
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
            if structure['type']!=None and str(structure['type']).__eq__('dw'):
                game_db='%s_dw' % game
                t_structure.append(game_db)
            elif structure['type']!=None and str(structure['type']).__eq__('dm'):
                game_db='%s_dm' % game
                t_structure.append(game_db)
            elif structure['type']!=None and str(structure['type']).__eq__('report'):
                game_db='report_%s' % game
                t_structure.append(game_db)
            exis_row=self.mysql.query("select id from structure where platform='%s' and is_delete=0 and db_name='%s' and platform='all' and table_name='%s' and db_type='%s'"%(plat_form,game_db,str(structure['table_name']),str(structure['db_type'])))
            if len(exis_row)>0:
                return  int(exis_row[0]['id'])
            else:
                return self.save_newstructure(t_structure)


    def save_new_task(self,task):
        self.mysql.insert("dw2dm",**task)
        self.mysql.commit()
    def save_newstructure(self,structure):
        query='INSERT INTO structure(type,flag,db_type,game,platform,table_name,column_name,create_table_sql,user_id,is_delete,create_date,db_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        rowNum=self.mysql.execute(query,*tuple(structure))
        self.mysql.commit()
        return rowNum
    def run(self,game,task_name=None,plat_form="all"):
        print "start copy"
        task_list = self.get_all_task(task_name)
        
        for task in task_list:
            form_ids=""
            for form_id_str in task['from_id'].split(","):
                if len(str(form_ids))>0:
                    form_ids=form_ids+","+str(self.get_structure(int(form_id_str),game,plat_form))
                else:
                    form_ids=str(self.get_structure(int(form_id_str),game,plat_form))
            target_id=self.get_structure(int(task['target_id']),game,plat_form)
            t_task = {
                'game':game,
                ##'platform':task['platform'],
                'platform':plat_form,
                'log_name':task['log_name'],
                'do_rate':task['do_rate'],
                'priority':task['priority'],
                'prefix_sql':task['prefix_sql'],
                'exec_sql':task['exec_sql'].replace("%s_dw" % self.source_game,"%s_dw" % game).replace("%s_dm" % self.source_game,"%s_dm" % game),
                'post_sql':task['post_sql'],
                'from_id':form_ids,
                'target_id':target_id,
                'create_date':datetime.datetime.today().strftime("%Y-%m-%d"),
                'comment':task['comment'],
                'grouped':task['grouped'],
                'is_delete':task['is_delete'],
                'user_id':task['user_id']
            }
            self.save_new_task(t_task)
        
        self.mysql.close()
        print "over"

    def add_structure(self,game,plat_form):
        platforms_str=plat_form.split(",")
        structures=self.mysql.query("select * from structure where platform='all' and is_delete=0 and flag='log' and game='ares' and type in ('report','dm')")
        for structure in structures:
            for platform in platforms_str:
                t_structure=[
                    structure['type'],
                    structure['flag'],
                    structure['db_type'],
                    game,
                    platform,
                    #structure['platform'],
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
                if structure['type']!=None and str(structure['type']).__eq__('dw'):
                    game_db='%s_dw' % game
                elif structure['type']!=None and str(structure['type']).__eq__('dm'):
                    game_db='%s_dm' % game
                elif structure['type']!=None and str(structure['type']).__eq__('report'):
                    game_db='report_%s' % game
                t_structure.append(game_db)
                self.save_newstructure(t_structure)
if __name__ == '__main__':

    parser = optparse.OptionParser()

    parser.add_option(
        '--game',
        dest='game',
        help='game name of task',
        metavar='GAME',
    )

    parser.add_option(
        '--open_cp_structure',
        dest='open_cp_structure',
        help='open_cp_structure',
        metavar='OPEN_CP_STRUCTURE',
    )

    parser.add_option(
        '--platform',
        dest='platform',
        help='platform add stucture platform can use.',
        metavar='PLATFORM',
    )
    parser.add_option(
        '--taskname',
        dest='taskname',
        help='task_name (add common task can use)',
        metavar='TASKNAME',
    )
    options, args = parser.parse_args()
    ###掌门
    #options.plat_form='xp,gamecomb,winphone,appstore,iosky,iostb,mix,ios91,appstoremix,appstoretw,winphonetw,qqandroid,iospp,kunlun'
    #options.plat_form='appstoremix'
    ###
    #options.open_cp_structure=0
    #options.table="dw2dm"
    ##options.game='kof'
    ##options.plat_form='korea'
    #options.game='hebe'
    #options.game='crius'
    ##options.game='xianpro'
    ##options.game='kof'
    task = CopyConfig()
    is_stru=options.open_cp_structure if options.open_cp_structure is not None else 'off'

    if options.game and is_stru =='off':
        task.run(
            game = options.game,
            task_name = options.taskname,
            plat_form=options.platform if options.platform is not None else "all"
        )
    elif options.platform and is_stru=='no':
        task.add_structure(
            game = options.game,
            plat_form=options.platform
        )
    else:
        parser.print_help()
