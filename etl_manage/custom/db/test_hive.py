#!/usr/bin/python
#coding=utf-8


from hive import Custom_Hive
#声明实例

hive = Custom_Hive('ares_dw')

#param = {}
#param['database_name'] = 'ares_dw'
#param['log_dir'] = '/tmp/test.txt'
#param['table_name'] = 'login_map'
#param['pt'] = "dt ='2015-09-16'"

#print hive.load(**param)


sql = 'set hive.support.concurrency=false;SET hive.exec.compress.output=true;SET mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;insert into table  ares_mart.test_lzo select uid,create_time from test_sgx.game_user_info group by uid,create_time  limit 1;'
result = hive.move(sql)

exit()
#演示 hive count 查询条数
sql = 'select * from  game_user_info limit 100'
result = hive.count(sql)
print result
#演示 hive query 查询结果集多条
sql = 'select * from  game_user_info limit 1'
result = hive.query(sql)
for i in result:

    print i
