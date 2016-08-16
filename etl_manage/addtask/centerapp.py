#!/usr/bin/python
#coding=utf-8

"""
auth:zhengyingying
mail:zhengyingying@playcrab.com
createtime:2015-10-13 10:00:00
usege: 用于生成etl_data 任务

"""

import datetime
from custom.db.mysql import Custom_MySQL

__ALL__=['CenterApp']
class CenterApp():
    def __init__(self):
        self.center_mysql = Custom_MySQL(using='center_app')
        
#     def get_server_list(self):
#         '''
#             获取执行任务
#         '''
#         try:
#             ip_list = []
#             if param['flag'] == 'basic':
#                 ip_list.append('120.26.1.250')
#                 
#             if param['flag'] == 'most':
#                 ip_list.append('120.26.13.150')
#             
#             if param['flag'] == 'log':
#                 s_sql = "select t1.*  from \
#                             (select a.public_ip as source_ip,a.platform_id from assets a inner join main_category m \
#                             on a.main_category_id = m.id  \
#                             where a.is_del = 0 and (a.hostname like '%%%%web_balance%%%%' or a.hostname like '%%%%web_admin%%%%') ) t1 \
#                             left join platform t2 on t1.platform_id = t2.id \
#                             where t1.source_ip is not null group by source_ip"
#                             
#                 if param['game'] == 'kof':
#                     s_sql = "select t1.*  from \
#                             (select a.public_ip as source_ip,a.platform_id from assets a inner join main_category m \
#                             on a.main_category_id = m.id  \
#                             where a.is_del = 0 and a.hostname like '%%%%gameserver%%%%' ) t1 \
#                             left join platform t2 on t1.platform_id = t2.id \
#                             where t1.source_ip is not null group by source_ip" 
#                  
#                 ip_list = self.exec_sql(s_sql)
#   
#             if param['flag'] == 'snap':
#                 s_sql = "select s.prefix as source_ip from sub_category s inner join main_category m \
#                         on s.main_category_id = m.id \
#                         where s.platform = '%s' and m.prefix = '%s'" % (param['platform'],param['game'])
#                 ip_list = self.exec_sql(s_sql)
#             
#             return ip_list
#         except Exception as exc:
#             print exc
#             #异常回滚
#             self.center_mysql.rollback()
    
    
    def get_log_ip(self):
        '''
            获取所有log的IP
        '''
        
        ip_list = {}
        sql = "select t1.*,t2.prefix as platform  from \
                            (select m.prefix as gamename,a.public_ip as source_ip,a.platform_id from assets a inner join main_category m \
                            on a.main_category_id = m.id  \
                            where a.is_del = 0 and (a.hostname like '%%%%web_balance%%%%' or a.hostname like '%%%%web_admin%%%%') ) t1 \
                            left join platform t2 on t1.platform_id = t2.id \
                            where t1.source_ip is not null group by source_ip\
                union all \
                select t1.*,t2.prefix as platform from \
                            (select m.prefix as gamename,a.public_ip as source_ip,a.platform_id from assets a inner join main_category m \
                            on a.main_category_id = m.id  \
                            where a.is_del = 0 and a.hostname like '%%%%gameserver%%%%' ) t1 \
                            left join platform t2 on t1.platform_id = t2.id \
                            where t1.source_ip is not null group by source_ip"
        
        result = self.center_mysql.query(sql)
  
        for info in result:
            if info['gamename'] is None or info['platform'] is None:
                continue
            
            if info['gamename'].encode('utf8') not in ip_list:
                ip_list[info['gamename'].encode('utf8')] = {}
            
            if info['platform'].encode('utf8') not in ip_list[info['gamename'].encode('utf8')]:
                ip_list[info['gamename'].encode('utf8')][info['platform'].encode('utf8')] = []
                
            ip_list[info['gamename'].encode('utf8')][info['platform'].encode('utf8')].append(info['source_ip'].encode('utf8'))

        return ip_list
    
    
    def get_snap_ip(self):
        '''
            获取所有快照的IP
        '''
        ip_list = {}
        s_sql = "select f.prefix as platform,s.prefix as source_ip,m.prefix as gamename from sub_category s inner join main_category m \
                        on s.main_category_id = m.id \
                left join platform f on f.id = s.platform_id"
        
        result = self.center_mysql.query(s_sql)
        
        for info in result:
            if info['gamename'] is None or info['platform'] is None:
                continue
            
            if info['gamename'].encode('utf8') not in ip_list:
                ip_list[info['gamename'].encode('utf8')] = {}
            
            if info['platform'].encode('utf8') not in ip_list[info['gamename'].encode('utf8')]:
                ip_list[info['gamename'].encode('utf8')][info['platform'].encode('utf8')] = []
                
            ip_list[info['gamename'].encode('utf8')][info['platform'].encode('utf8')].append(info['source_ip'].encode('utf8'))
        
#         print ip_list
        return ip_list
    
        
    def get_ip_list(self):
        '''
            整理IP列表
        '''
        try:
            ip_list = {}
          
            ip_list['basic'] = ['120.26.1.250']
                
            ip_list['mostsdk'] = ['120.26.13.150']

            ip_list['wanpay'] = ['112.124.116.44']
            
            log_list = self.get_log_ip();
             
            ip_list['log'] = log_list

            snap_list = self.get_snap_ip()
            ip_list['snap'] = snap_list
            
            return ip_list
        except Exception,e:
            print e
            print "异常"
            #异常回滚
            self.center_mysql.rollback()
        

    def close_mysql(self):
        '''
            关闭数据库
        '''
        self.center_mysql.close();
