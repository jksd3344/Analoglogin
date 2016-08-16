#!/usr/bin/python
#coding=utf-8

"""
auth:suguoxin
mail:suguoxin@playcrab.com
createtime:2016-01-12 00:30:00
usege: 用于自动重置执行失败的任务

"""

import datetime
from custom.db.mysql import Custom_MySQL

mysql = Custom_MySQL(using='etl_manage')

#当前时间
now_time = datetime.datetime.now()
task_date = now_time.strftime('%Y%m%d')
log_time = now_time.strftime('%H%M')

if log_time < '0200':
    task_date = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

log_time_before_30min = (now_time-datetime.timedelta(seconds=1800)).strftime('%H%M')
log_time_before_1hour = (now_time-datetime.timedelta(seconds=3600)).strftime('%H%M')

now = now_time.strftime('%Y-%m-%d %H:%M:%S')

try:
    mysql.begin()

    #重置etl_status
    etl_result = mysql.update_by_sql('update etl_data_log set etl_status=0,etl_exec_num=0,in_etl_queue=0 '
                                     'where etl_status=-1 and task_date="%s"' % task_date)
    if etl_result['status'] != 0:
        print(now+" restart etl task error")

    etl_result_1 = mysql.update_by_sql('update etl_data_log set etl_status=0,etl_exec_num=0,in_etl_queue=0 '
                                       'where etl_status!=6 and etl_status!=-2 and in_etl_queue=1 and log_time <="%s" and task_date="%s"'
                                       '' % (log_time_before_30min, task_date))
    if etl_result_1['status'] != 0:
        print(now+" restart etl_1 task error")

    #重置download_status
    download_result = mysql.update_by_sql('update etl_data_log set download_status=0,download_exec_num=0,in_download_queue=0 '
                                          'where download_status=-1 and task_date="%s"' % task_date)
    if download_result['status'] != 0:
        print(now+" restart download task error")

    download_result_1 = mysql.update_by_sql('update etl_data_log set download_status=0,download_exec_num=0,in_download_queue=0 '
                                            'where download_status!=3 and download_status!=-2 and in_download_queue=1 and log_time <="%s" and task_date="%s"'
                                            '' % (log_time_before_30min, task_date))
    if download_result_1['status'] != 0:
        print(now+" restart download_1 task error")

    #重置file2mysql_status
    file2mysql_result = mysql.update_by_sql('update file2mysql_log set load_status=0,exec_num=0,in_queue=0 '
                                            'where load_status=-1 and task_date="%s"' % task_date)
    if file2mysql_result['status'] != 0:
        print(now+" restart file2mysql task error")

    file2mysql_result_1 = mysql.update_by_sql('update file2mysql_log set load_status=0,exec_num=0,in_queue=0 '
                                              'where load_status!=3 and load_status!=-2 and in_queue=1 and log_time <="%s" and task_date="%s"'
                                              '' % (log_time_before_30min, task_date))
    if file2mysql_result_1['status'] != 0:
        print(now+" restart file2mysql_1 task error")

    #重置file2dw_status
    if log_time[-2:] == '03':

        file2dw_result = mysql.update_by_sql('update file2dw_log set load_status=0,exec_num=0,in_queue=0 '
                                             'where load_status=-1 and task_date="%s"' % task_date)
        if file2dw_result['status'] != 0:
            print(now+" restart file2dw task error")

        file2dw_result_1 = mysql.update_by_sql('update file2dw_log set load_status=0,exec_num=0,in_queue=0 '
                                               'where load_status!=5 and in_queue=1 and log_time <="%s" and task_date="%s"'
                                               '' % (log_time_before_1hour, task_date))
        if file2dw_result_1['status'] != 0:
            print(now+" restart file2dw_1 task error")

    if '0500' < log_time < '1100':
        #重置dm2report_status
        dm2report_result = mysql.update_by_sql('update dm2report_log set status=0,exec_num=0,in_queue=0 '
                                               'where status=-1 and task_date="%s"' % task_date)
        if dm2report_result['status'] != 0:
            print(now+" restart dm2report task error")

        dm2report_result_1 = mysql.update_by_sql('update dm2report_log set status=0,exec_num=0,in_queue=0 '
                                                 'where status!=4 and in_queue=1 and log_time <="%s" and task_date="%s"'
                                                 '' % (log_time_before_1hour, task_date))
        if dm2report_result_1['status'] != 0:
            print(now+" restart dm2report_1 task error")

    mysql.commit()
    mysql.close()

except Exception as exc:
    #回滚
    mysql.rollback()
    print(exc)
    mysql.close()
