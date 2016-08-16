#!/usr/bin/python
#coding=utf-8

"""
auth:suguoxin
mail:suguoxin@playcrab.com
createtime:2016-02-17 18:30:00
usege: 监控数据采集的任务，并发送邮件及短信告警

"""

import datetime
from custom.db.mysql import Custom_MySQL
from custom.command import Custom_Command as cmd
import ConfigParser

import sys
reload(sys)
sys.setdefaultencoding('utf8')

#项目目录
#project_path = os.getcwd()
project_path = "/data/etl_manage"

#读取配置文件，获取白名单信息(web端可考虑移植到数据库，或自动生成配置文件)
conf = ConfigParser.ConfigParser()
conf.read(project_path+"/conf/monitor_white_list.ini")

#读取配置文件，获取告警接收人的信息
conf_alarm = ConfigParser.ConfigParser()
conf_alarm.read(project_path+"/conf/alarm_user_list.ini")

#接收告警短信的电话号码，如多个以逗号分隔即可
alarm_tel = conf_alarm.get('telephone', 'value')
#接收告警邮件的邮件地址，如多个以逗号分隔即可
alarm_email = conf_alarm.get('email', 'value')

#时间常量
LOG_TIME = '0130'

#获取系统当前时间
now_time = datetime.datetime.now()
task_date = now_time.strftime('%Y%m%d')
log_time = now_time.strftime('%H%M')
log_time_before_2hour = (now_time-datetime.timedelta(seconds=7200)).strftime('%H%M')

'''
etl_data 任务监控
'''
def etl_data():

    #获取白名单信息
    source_ip_list = conf.get('etl_data', 'source_ip')
    role_day_info_lost = conf.get('etl_data', 'role_day_info')

    sql = "select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
          "where task_date='%s' and log_time<='%s' and log_time>='%s' and etl_status=-2" \
          "" % (task_date, log_time, log_time_before_2hour)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and etl_status=-2) " \
              "union (game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and log_time>='%s' and etl_status=-2) " \
              % (task_date_old, log_time_old, task_date, log_time, log_time_before_2hour)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        if res['source_ip'] not in source_ip_list and res['game']+"_"+res['platform']+"_"+res['source_ip'] not in role_day_info_lost:
            content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['source_ip']+"-"+res['task_date']+"_"+res['log_time'] + "..."
            if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
                content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("etl_data", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("etl_data", content_email)

    return True

'''
download 任务监控
'''
def download():

    #获取白名单信息
    source_ip_list = conf.get('download', 'source_ip')
    role_day_info_lost = conf.get('download', 'role_day_info')

    sql = "select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
          "where task_date='%s' and log_time<='%s' and log_time>='%s' and download_status=-2" \
          "" % (task_date, log_time, log_time_before_2hour)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and download_status=-2) " \
              "union (game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and log_time>='%s' and download_status=-2) " \
              % (task_date_old, log_time_old, task_date, log_time, log_time_before_2hour)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        if res['source_ip'] not in source_ip_list and res['game']+"_"+res['platform']+"_"+res['source_ip'] not in role_day_info_lost:
            content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['source_ip']+"-"+res['task_date']+"_"+res['log_time'] + "..."
            if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
                content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("download", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("download", content_email)

    return True

'''
mergefile2dw 任务监控
'''
def mergefile2dw():

    sql = "select game,platform,log_name,log_time,task_date from file2dw_log " \
          "where task_date='%s' and log_time<='%s' and load_status=-1" % (task_date, log_time)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, log_time, task_date from file2dw_log " \
              "where task_date='%s' and log_time<='%s' and load_status=-1) " \
              "union (game,platform,log_name, log_time, task_date from file2dw_log " \
              "where task_date='%s' and log_time<='%s' and load_status=-1) " \
              % (task_date_old, log_time_old, task_date, log_time)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['task_date']+"_"+res['log_time'] + "..."
        if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
            content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("mergefile2dw", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("mergefile2dw", content_email)

    return True

'''
file2mysql 任务监控
'''
def file2mysql():

    sql = "select game,platform,log_name, source_ip, log_time, task_date from file2mysql_log " \
          "where task_date='%s' and log_time<='%s' and log_time >='%s' and load_status=-2" \
          "" % (task_date, log_time, log_time_before_2hour)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, source_ip, log_time, task_date from file2mysql_log " \
              "where task_date='%s' and log_time<='%s' and load_tatus=-2) " \
              "union (game,platform,log_name, source_ip, log_time, task_date from file2mysql_log " \
              "where task_date='%s' and log_time<='%s' and log_time >='%s' and load_status=-2) " \
              % (task_date_old, log_time_old, task_date, log_time, log_time_before_2hour)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['source_ip']+"-"+res['task_date']+"_"+res['log_time'] + "..."
        if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
            content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("file2mysql", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("file2mysql", content_email)

    return True

'''
file2dw 任务监控
'''
def file2dw():

    sql = "select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
          "where task_date='%s' and log_time<='%s' and etl_status=-2" % (task_date, log_time)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and etl_status=-2) " \
              "union (game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and etl_status=-2) " \
              % (task_date_old, log_time_old, task_date, log_time)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['source_ip']+"-"+res['task_date']+"_"+res['log_time'] + "..."
        if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
            content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("file2dw", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("file2dw", content_email)

    return True

'''
dw2dm 任务监控
'''
def dw2dm():

    sql = "select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
          "where task_date='%s' and log_time<='%s' and etl_status=-2" % (task_date, log_time)

    if log_time < LOG_TIME:
        log_time_old = '2400'
        task_date_old = (now_time-datetime.timedelta(days=1)).strftime('%Y%m%d')

        sql = "(select game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and etl_status=-2) " \
              "union (game,platform,log_name, source_ip, log_time, task_date from etl_data_log " \
              "where task_date='%s' and log_time<='%s' and etl_status=-2) " \
              % (task_date_old, log_time_old, task_date, log_time)

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        content_email += res['game']+"-"+res['platform']+"-"+res['log_name']+"-"+res['source_ip']+"-"+res['task_date']+"_"+res['log_time'] + "..."
        if res['game']+"-"+res['platform']+"-"+res['log_name'] not in content_ms:
            content_ms += res['game']+"-"+res['platform']+"-"+res['log_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("dw2dm", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("dw2dm", content_email)

    return True


'''
dm2report 任务监控
'''
def dm2report():

    sql = "select game,platform,task_name, date_cycle, task_date from dm2report_log " \
          "where task_date='%s' and status=-1" % task_date

    result = query_mysql(sql)

    content_email = ""
    content_ms = ""
    for res in result:
        content_email += res['game']+"-"+res['platform']+"-"+res['task_name']+"-"+res['date_cycle']+"-"+res['task_date'] + "..."
        if res['game']+"-"+res['platform']+"-"+res['task_name'] not in content_ms:
            content_ms += res['game']+"-"+res['platform']+"-"+res['task_name'] +"..."

    if content_ms is not None and content_ms != "":
        send_alarm_ms("dm2report", content_ms)
    if content_email is not None and content_email != "":
        send_alarm_email("dm2report", content_email)

    return True

'''
查询数据库
'''
def query_mysql(sql):

    mysql = Custom_MySQL(using='etl_manage')

    try:
        mysql.begin()
        result = mysql.query(sql)

        mysql.commit()
        mysql.close()

        return result

    except Exception as exc:
        #回滚
        mysql.rollback()
        print(exc)
        mysql.close()

'''
发送短信
'''
def send_alarm_ms(task_name, content_ms):

    sendms = cmd.run('/usr/bin/php /data/etl_manage/custom/sendmessage.php '+alarm_tel+''
                     ' "[ERROR] 数据采集'+task_name+'过程异常，具体如下：'+content_ms+'"')
    if sendms['status'] != 0:
        return False

    return True

'''
发送邮件
'''
def send_alarm_email(task_name, content_email):

    sendemail = cmd.run('/usr/bin/python /data/etl_manage/send_email.py -t '+alarm_email+''
                        ' -i "[ERROR]  数据采集" -o "[ERROR]  数据采集'+task_name+'过程异常，具体如下： "'+content_email)
    if sendemail['status'] != 0:
        return False

    return True

if __name__ == '__main__':
    #print()
    etl_data()
    download()
    #mergefile2dw()
    file2mysql()
    #file2dw()
    #dw2dm()
    #dm2report()