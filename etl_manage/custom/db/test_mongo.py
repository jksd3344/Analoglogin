#/usr/bin/python
#coding=utf-8

'''
auth:wuqichao@playcrab.com
date:2014-06-17 12:00

'''
from custom.db.mongo import Custom_Mongo

n = Custom_Mongo(using='logs')

param = {'sex':'man','name':'wuqichao'}
print n.insert('users',**param)

param = {'sex':'man','name':'wuqichao'}
print n.get('users',**param)

param = {'sex':'man','name':'wuqichao'}
print n.count('users', **param)

n.update('users',{'sex':'man','name':'wuqichao'},**{'sex':'wman','name':'wuqichao'})

param = {'sex':'man','name':'wuqichao'}
res =  n.query('users',param,skip=2,limit=2)
for r in res:
    print r
    
  