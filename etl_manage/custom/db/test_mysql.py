#/usr/bin/python
#coding=utf-8


from mysql import Custom_MySQL
#声明实例

mysql = Custom_MySQL('test')
sql = "SELECT * from ad_bill limit 10"
result = mysql.dump(sql,'/tmp/test1020.txt')
print(result)


#演示load
sql = "Load Data Local InFile '/tmp/test1020.txt' Into Table TableTest"
result = mysql.load(sql)
print(result)
exit()

#演示 mysql count 查询条数
sql = 'select count(*) as count from qiubai'
result = mysql.count(sql)
count = result['count']
print(count)

#演示 mysql query 查询结果集多条
sql = 'select id,url from qiubai limit 10'
result = mysql.query(sql)
for i in result:

    print (i['id'],i['url'])

#演示 mysql get  查询结果集只有一条
sql = 'select id,url from qiubai limit 10'
result = mysql.get(sql)
print (result)

#事务处理
mysql.begin()
try:
    datas ={
        'year':'2013',
        'month':'10',
        'day':'12',
        'page':'80',
        'index':'2'
        }

    #演示 mysql insert  返回last_insert_id
    print (mysql.insert('qiubai',**datas))
    datas.update({'url':'wwwwwwwwwww'})

    #演示 mysql update  返回update的影响数
    mysql.update('qiubai',
                      ' year = %(year)s and  month =%(month)s and day = %(day)s and  page=%(page)s and `index`=%(index)s'%datas,
                      **datas)
    #演示 mysql delete  返回delete的影响数
    mysql.delete('qiubai',

                      ' year = %(year)s and  month =%(month)s and day = %(day)s and  page=%(page)s and `index`=%(index)s'%datas)

    #提交事务
    mysql.commit()
except:
    #回滚
    mysql.rollback()