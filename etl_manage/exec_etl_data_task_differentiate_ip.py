#!/usr/bin/python
#coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-04-26 12:00:00
used: 用于清洗游戏日志原始数据，区分机器（收集机 仅收集指定ip的服务器数据）
       主要用于应对：一个平台的服务器过多，一台收集机无法完成任务的情况

last_update: 2016-04-28 14:54:00
"""

import datetime
import sys
from etl_data.tasks import run_task
from custom.db.mysql import Custom_MySQL
from custom.db.redis_tools import Custom_Redis

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

mysql = Custom_MySQL(using='etl_manage')
redis = Custom_Redis(using='etl_manage')

game = sys.argv[1]
platform = sys.argv[2]
do_rate = sys.argv[3]
now_time = sys.argv[4]
now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')

#数据日期，格式如：20151015
log_date = now_time.strftime('%Y%m%d')
#数据时间点(每五分钟)，格式如：0005、2400
log_time = now_time.strftime("%H%M")

#当前机器的外网ip
machine = "120.26.1.224"

#从redis中，获取当前机器所负责收集的
if redis.get("machine_xml") is None:
    machine_tmp = open('/data/etl_manage/conf/etl_machine.xml', 'r')
    redis.set("machine_xml", str(machine_tmp.read()))

machine_list = str(redis.get("machine_xml"))
root = ET.fromstring(machine_list)
ips = ""
for info in root.findall('game'):
    if info.get('name') == game and info.get('platform') == platform and info.get('machine') == machine:
        ips = "'" + str(info.get('ips')).replace(',', '\',\'') + "'"
        continue

sql = 'select * from etl_data_log ' \
      'where game="%s" and platform="%s" and do_rate="%s" and source_ip in (%s) and etl_status=0 and task_date="%s" ' \
      'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s"' \
      '' % (game, platform, do_rate, ips, log_date, log_time)

if log_time <= '0100':
    log_time_old = '2400'
    log_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')
    sql = '(select * from etl_data_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and source_ip in (%s) and etl_status=0 and task_date="%s" ' \
          'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s") ' \
          'union (select * from etl_data_log ' \
          'where game="%s" and platform="%s" and do_rate="%s" and source_ip in (%s) and etl_status=0 and task_date="%s" ' \
          'and in_etl_queue =0 and etl_exec_num < 4 and etl_retry_num < 6 and log_time <="%s")' \
          '' % (game, platform, do_rate, ips, log_date, log_time, game, platform, do_rate, ips, log_date_old, log_time_old)

tasks = mysql.query(sql)

for task in tasks:
    #print(task)
    try:
        mysql.begin()
        where = {'id': int(task['id'])}

        '''
        照理说for里面不会有人抢快照数据，以防万一起动排他锁(使用主键启动行锁)，兄弟们最好别瞎用
        '''
        #result = mysql.get('select * from etl_data_log where etl_status = 0 and id = %(id)s for update' % where)
        #result = mysql.get('select * from etl_data_log where etl_status = 0 and id = %(id)s ' % where)
        result = mysql.get('select f.*,s.flag from etl_data_log as f left join structure as s '
                           'on f.target_id=s.id where f.id = %(id)s ' % where)

        if result is not None:
            etl_exec_num = int(result['etl_exec_num'])
            in_etl_queue = int(result['in_etl_queue'])
            etl_retry_num = int(result['etl_retry_num'])

            if etl_retry_num < 5:
                if in_etl_queue == 0:
                    if etl_exec_num < 3:
                        if etl_exec_num == 0:
                            etl_retry_num += 1

                        etl_exec_num += 1
                        in_etl_queue = 1

                        datas = {'etl_exec_num': etl_exec_num, 'in_etl_queue': in_etl_queue, 'etl_retry_num': etl_retry_num}
                        update_result = mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
                        if update_result == 1:
                            run_task.apply_async((result,), queue='etl_data')
                        else:
                            print "update data{'etl_exec_num', 'in_etl_queue', 'etl_retry_num'} error"
                    else:
                        #任务在该次自动重试中执行失败
                        datas = {'etl_status': -1}
                        mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)
            else:
                #任务执行彻底失败，不再自动重试
                datas = {'etl_status': -2}
                mysql.update('etl_data_log', ' id = %(id)d' % where, **datas)

        mysql.commit()

    except Exception as exc:
        print(exc)
        mysql.rollback()
        print "db rollback success"
        mysql.close()

