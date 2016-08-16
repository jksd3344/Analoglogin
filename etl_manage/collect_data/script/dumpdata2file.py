#coding=utf-8
import threading
from time import ctime,sleep
from datetime import datetime,timedelta
import commands
from os import path,makedirs
import getopt
import sys

def get_bill_data(list_data):
    sql=None
    if (list_data['mode'] == 'retry' or list_data['mode'] == 'normal'):
        sql='''set names 'utf8';select id as order_id,app_id,cash,cash_type,account_id,product_id,product_num,product_price,product_name,role_id,sec as dist,create_time,sdk_source,type,status,partner_order_id,device_id,pay_time,finish_time,pay_remote_ip,pay_forwarded_ip,from_remote_ip,from_forwarded_ip,create_date from bill where create_time >= '{from_date}' and create_time < '{to_date}';'''.format(from_date=list_data['v_fromdate'],to_date=list_data['v_todate'])
    elif (list_data['mode'] == 'all'):
        sql='''set names 'utf8';select id as order_id,app_id,cash,cash_type,account_id,product_id,product_num,product_price,product_name,role_id,sec as dist,create_time,sdk_source,type,status,partner_order_id,device_id,pay_time,finish_time,pay_remote_ip,pay_forwarded_ip,from_remote_ip,from_forwarded_ip,create_date from bill where create_time <= '{to_date}';'''.format(to_date=list_data['v_todate'])
    output_path = "/data/log_data/mostsdk/info/" + list_data['v_today'] + "/recharge_mostsdk_log"
    file_name = "recharge_mostsdk_log_120.26.13.150_" + list_data['v_minutes'] + ".log"
    print output_path,file_name
    print sql
    sleep(240)
    check_dirs(output_path)
    exec_sql(sql,output_path,file_name)
    md5(output_path,file_name)
    sleep(5)
def get_account_data(list_data):
    sql=None
    if (list_data['mode'] == 'retry' or list_data['mode'] == 'normal'):
        sql='''set names 'utf8';select a.account_id,d.main_category_id as game_id,d.platform_id as platform_id,first_login_ip,first_login_device_id,first_login_time,case when e.account_id is null then 0 else 1 end as type,d.channel_id as sdk_source from account_app as a left outer join app d on a.app_id = d.id left outer join inside_account e on a.account_id = e.account_id and a.app_id = e.app_id where first_login_time >= '{from_date}' and first_login_time < '{to_date}';'''.format(from_date=list_data['v_fromdate'],to_date=list_data['v_todate'])
    elif (list_data['mode'] == 'all'):
        sql='''set names 'utf8';select a.account_id,d.main_category_id as game_id,d.platform_id as platform_id,first_login_ip,first_login_device_id,first_login_time,case when e.account_id is null then 0 else 1 end as type,d.channel_id as sdk_source from account_app as a left outer join app d on a.app_id = d.id left outer join inside_account e on a.account_id = e.account_id and a.app_id = e.app_id where first_login_time  <= '{to_date}';'''.format(to_date=list_data['v_todate'])
    output_path = "/data/log_data/mostsdk/info/" + list_data['v_today'] + "/account_info_sdk"
    file_name = "account_info_sdk_120.26.13.150_" + list_data['v_minutes'] + ".log"
    print output_path,file_name
    print sql
    sleep(240)
    check_dirs(output_path)
    exec_sql(sql,output_path,file_name)
    md5(output_path,file_name)
    sleep(5)
def get_login_data(list_data):
    sql=None
    if (list_data['mode'] == 'retry' or list_data['mode'] == 'normal'):
        sql='''set names 'utf8';select account_id,main_category_id as game,platform_id as platform,login_forwarded_ip as login_ip,device_id as login_device_id,login_time from login_log where login_time >= '{from_date}' and login_time < '{to_date}';'''.format(from_date=list_data['v_fromdate'],to_date=list_data['v_todate'])
    elif (list_data['mode'] == 'all'):
        sql='''set names 'utf8';select account_id,main_category_id as game,platform_id as platform,login_forwarded_ip as login_ip,device_id as login_device_id,login_time from login_log where login_time  <= '{to_date}';'''.format(to_date=list_data['v_todate'])
    output_path = "/data/log_data/mostsdk/info/" + list_data['v_today'] + "/account_login_log"
    file_name = "account_login_log_120.26.13.150_" + list_data['v_minutes'] + ".log"
    print output_path,file_name
    print sql
    sleep(240)
    check_dirs(output_path)
    exec_sql(sql,output_path,file_name)
    md5(output_path,file_name)
    sleep(5)
def exec_sql(v_sql,v_output_path,v_filename):
    cmd='''mysql -h120.26.13.150 -P3306 -uread_mostsdk -p'TEST_READ_MOSTSDK' mostsdk -N -e "{sql}" > {output_path}/{file_name}'''.format(sql=v_sql,output_path=v_output_path,file_name=v_filename)
    print cmd
    (status,output) = commands.getstatusoutput(cmd)
def check_dirs(dir):
    print dir
    status=path.exists(dir)
    print status
    if (not status):
        makedirs(dir)
def md5(v_output_path,v_filename):
    cmd='''md5sum {output_path}/{file_name} > {output_path}/{file_name}.md5'''.format(output_path=v_output_path,file_name=v_filename)
    print cmd
    (status,output) = commands.getstatusoutput(cmd)
def main(argv):
    try:
        opts,args = getopt.getopt(sys.argv[1:],'hnart:d:',['help','rerty','all','normal','table','datetime'])
    except getopt.GetoptError,err:
        print str(err)
        sys.exit(2)
    threads = []
    now = datetime.now()
    VARS = {}
    VARS['v_today'] = None
    VARS['v_fromdate'] = None
    VARS['v_todate'] = None
    VARS['v_minutes'] = None
    VARS['table'] = None
    VARS['mode'] = None
    for opt, value in opts:
        if opt in ('-h','--help'):
            print "-n 加载当前时间点前5分钟所有表的数据."
            print "-a 加载当前时间点前所有表的数据."
            print "-r 指定重跑某个时间点前5分钟和对应表的数据.例如: -r -d 2015-12-02 01:00 -t account"
            print "-t 指定重跑数据的表名."
            print "-d 指定重跑数据的时间."
            sys.exit()
        if opt in('-n','--normal'):
            '''定义四个时间变量，
		1个为当前日期为访问本地路径使用:v_today
		2个时间变量为查询sql使用:v_fromdate and v_todate
		1个为记录文件时的标识使用:v_minutes
            '''
            VARS['mode'] = 'normal'
            VARS['v_minutes'] = now.strftime("%H%M")
            if (VARS['v_minutes'] =='0000'):
                VARS['v_today'] = (now - timedelta(days=1)).strftime("%Y%m%d")
                VARS['v_minutes'] = "2400"
            else:
                VARS['v_today'] = now.strftime("%Y%m%d")
            VARS['v_todate'] = now.strftime("%Y-%m-%d %H:%M")
            fromdate = now - timedelta(minutes=5)
            VARS['v_fromdate'] = fromdate.strftime("%Y-%m-%d %H:%M")
            t1 = threading.Thread(target=get_bill_data,args=(VARS,))
            threads.append(t1)
            t2 = threading.Thread(target=get_account_data,args=(VARS,))
            threads.append(t2)
            t3 = threading.Thread(target=get_login_data,args=(VARS,))
            threads.append(t3)
            for t in threads:
                t.setDaemon(True)
                t.start()
            t.join()
        elif opt in ('-t','table'):
            VARS['table'] = value
        elif opt in ('-d','datetime'):
            day = value
            try:
                day = datetime.strptime(day, '%Y-%m-%d %H:%M')
            except ValueError, e:
                print e
                sys.exit()
            VARS['v_minutes'] = day.strftime("%H%M")
            if (VARS['v_minutes'] =='0000'):
                VARS['v_today'] = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                VARS['v_minutes'] = "2400"
            else:
                VARS['v_today'] = now.strftime("%Y-%m-%d")
            VARS['v_todate'] = day.strftime("%Y-%m-%d %H:%M")
            fromdate = day - timedelta(minutes=5)
            VARS['v_fromdate'] = fromdate.strftime("%Y-%m-%d %H:%M")
        elif opt in('-r','retry'):
            VARS['mode'] = 'retry'
        elif opt in('-a','all'):
            VARS['mode'] = 'all'
            VARS['v_today'] = now.strftime("%Y-%m-%d")
            VARS['v_todate'] = now.strftime("%Y-%m-%d %H:%M")
            VARS['v_minutes'] = now.strftime("%H%M")
            t1 = threading.Thread(target=get_bill_data,args=(VARS,))
            threads.append(t1)
            t2 = threading.Thread(target=get_account_data,args=(VARS,))
            threads.append(t2)
            #t3 = threading.Thread(target=get_login_data,args=(VARS,))
            #threads.append(t3)
            for t in threads:
                t.setDaemon(True)
                t.start()
            t.join()

    if (VARS['mode'] == 'retry'):
        if (VARS['table'] == 'recharge_mostsdk_log'):
            get_bill_data(VARS)
        elif (VARS['table'] == 'account_info_sdk'):
            get_account_data(VARS)
        elif  (VARS['table'] == 'account_login_log'):
            get_login_data(VARS)
    
if __name__ == '__main__':
    main(sys.argv)
