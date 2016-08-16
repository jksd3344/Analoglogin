#!/usr/bin/python
#coding=utf-8

"""
auth:weixingxin
mail:weixingxin@playcrab.com
createtime:2016-02-18 10:00:00
usege: 创建报告mysql分区

"""
import optparse
import datetime,time,calendar
import re
from custom.db.mysql import Custom_MySQL
__ALL__=['AddPartition']
class AddPartition():
    def __init__(self):
        #self.mysql= {'report_ares': Custom_MySQL(using='report_ares'),'report_hebe': Custom_MySQL(using='report_hebe'),'report_crius': Custom_MySQL(using='report_crius')}
         self.mysql=Custom_MySQL(using='hadoops2') 
    def execPartitons(self,games,tables,start_day,end_day):
        ##conv db
        for game in games:
            db="report_"+game
            for table in tables:
                self.mysql.begin();
                do_date=start_day
                i=0;
                exec_partions_sql="ALTER TABLE "+db+"."+table+" ADD PARTITION ("
                patition_sql="";
                while do_date <= end_day:
                    i = i + 1
                    partition_name="p"+str(do_date).replace('-','');
                    is_exist=self.find_partition(db,table,partition_name)
                    if not is_exist:
                        patition_sql=patition_sql+"PARTITION %s VALUES LESS THAN (to_days('%s')),"%(partition_name,do_date)
                        #print patition_sql
                    do_date = start_day + datetime.timedelta(days = i)
                if len(patition_sql)>0:
                    replace_reg = re.compile(r',$')
                    print "add partition db:%s table:%s ,start_day:%s,end_day:%s"%(db,table,start_day,end_day)
                    sql=exec_partions_sql+replace_reg.sub('', patition_sql)+");"
                    print sql
                    self.mysql.execute(sql)
                    self.mysql.commit();

    def add_months(self,sourcedate,months):
        month = sourcedate.month - 1 + months
        year = int(sourcedate.year + month / 12 )
        month = month % 12 + 1
        day = min(sourcedate.day,calendar.monthrange(year,month)[1])
        return datetime.date(year,month,day)
    def find_partition(self,db,table_name,partition_name):
       # exis_row=self.mysql.query("select partition_name,partition_expression, partition_description,table_rows  from information_schema.partitions"\
        #                          " where table_schema = schema() and table_schema='%s' and table_name='%s' and partition_name='%s';"%(db,table_name,partition_name))
        exis_row=self.mysql.query("select partition_name,partition_expression, partition_description,table_rows  from information_schema.partitions"\
                                  " where table_schema='%s' and table_name='%s' and partition_name='%s';"%(db,table_name,partition_name))
        if len(exis_row)>0:
            print "exis partitons db:%s,table:%s,p_name:%s"%(db,table_name,partition_name)
            return True
        return False
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option(
        '--games',
        dest='games',
        help='game name of task',
        metavar='GAMES',
    )

    parser.add_option(
        '--dt',
        dest='dt',
        help='dt name of task',
        metavar='DT',
    )

    parser.add_option(
        '--tables',
        dest='tables',
        help='game name of task',
        metavar='TABLES',
    )

    options, args = parser.parse_args()
    if(options.games is None):
        options.games="ares,hebe,crius,kof,xianpro,master"
    if(options.tables is None):
        options.tables="report_active_device,report_active_device_retain,report_active_user,report_active_user_retain" \
                      ",report_add_device,report_add_user,report_consume,report_consume_number,report_device_retain" \
                      ",report_pay_device,report_pay_money,report_pay_time,report_pay_user_count,report_total_user" \
                      ",report_trid_currency_people,report_trid_current_gold,report_user_retain"\
                      ",report_payfirst_user_count,report_pay_adduser_count,report_payfirst_user_money,report_pay_ltv" \
                       ",report_pay_adduser_money,report_consume_ladder,report_consume_number_ladder,report_maxvip_loss_monitor" \
                      ",report_vip_loss_monitor,report_totaluser_level,report_activeuser_level,report_adduser_level"
    if(options.dt is None):
        options.dt=time.strftime('%Y%m%d',time.localtime(time.time()))
    #else:
    dt = datetime.datetime.strptime(options.dt,"%Y%m%d")

    partition=AddPartition();
    befor_month_1=partition.add_months(dt,1);
    befor_month_2=partition.add_months(dt,2);
    next_month_first_day= datetime.date(befor_month_1.year,befor_month_1.month,1);
    next_month_last_day= datetime.date(befor_month_2.year,befor_month_2.month,1)-datetime.timedelta(1);
    #print  next_month_first_day,next_month_last_day
    print "start add partition"
    partition.execPartitons(tuple(options.games.split(",")),tuple(options.tables.split(",")),next_month_first_day,next_month_last_day)
    print "end add partition"
