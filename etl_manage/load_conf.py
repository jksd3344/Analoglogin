#!/usr/bin/python
# coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
create_time: 2016-03-31 17:12:00
used: 加载配置文件到redis中

last_update: 2016-05-04 15:40:00
"""

from custom.db.redis_tools import Custom_Redis

"""
将游戏时差的配置文件加载到redis当中
"""
def load_jetlag_to_redis():
    redis = Custom_Redis(using='etl_manage')

    if redis.get("jetlag_xml") is None:
        jetlag_tmp = open('/data/etl_manage/conf/game_platform_jetlag.xml', 'r')
        redis.set("jetlag_xml", str(jetlag_tmp.read()))
    else:
        redis.delete("jetlag_xml")
        jetlag_tmp = open('/data/etl_manage/conf/game_platform_jetlag.xml', 'r')
        redis.set("jetlag_xml", str(jetlag_tmp.read()))

"""
将游戏数据存储在本地磁盘的位置信息配置文件加载到redis当中
"""
def load_disk_to_redis():
    redis = Custom_Redis(using='etl_manage')

    if redis.get("disk_xml") is None:
        disk_tmp = open('/data/etl_manage/conf/disk_game.xml', 'r')
        redis.set("disk_xml", str(disk_tmp.read()))
    else:
        redis.delete("disk_xml")
        disk_tmp = open('/data/etl_manage/conf/disk_game.xml', 'r')
        redis.set("disk_xml", str(disk_tmp.read()))

"""
将收集机 -- 服务器对应的配置文件 加载到redis当中
"""
def load_machine_to_redis():
    redis = Custom_Redis(using='etl_manage')

    if redis.get("machine_xml") is None:
        machine_tmp = open('/data/etl_manage/conf/etl_machine.xml', 'r')
        redis.set("machine_xml", str(machine_tmp.read()))
    else:
        redis.delete("machine_xml")
        machine_tmp = open('/data/etl_manage/conf/etl_machine.xml', 'r')
        redis.set("machine_xml", str(machine_tmp.read()))

"""
将收集机 -- table 对应的配置文件 加载到redis当中
"""
def load_table_to_redis():
    redis = Custom_Redis(using='etl_manage')

    if redis.get("table_xml") is None:
        table_tmp = open('/data/etl_manage/conf/etl_table.xml', 'r')
        redis.set("table_xml", str(table_tmp.read()))
    else:
        redis.delete("table_xml")
        table_tmp = open('/data/etl_manage/conf/etl_table.xml', 'r')
        redis.set("table_xml", str(table_tmp.read()))

if __name__ == '__main__':
    load_jetlag_to_redis()
    load_disk_to_redis()
    load_machine_to_redis()
    load_table_to_redis()