#!/usr/bin/python
# coding=utf-8

"""
auth: suguoxin
mail: suguoxin@playcrab.com
createtime: 2016-03-31 15:30:00
usege: 处理游戏平台时差

"""

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import sys
import getopt
from custom.db.redis_tools import Custom_Redis

"""
从redis中获取时差配置的xml文件
"""
def get_xml_from_redis():

    redis = Custom_Redis(using='etl_manage')

    if redis.get("jetlag_xml") is None:
        jetlag_tmp = open('/data/etl_manage/conf/game_platform_jetlag.xml', 'r')
        redis.set("jetlag_xml", str(jetlag_tmp.read()))

    return str(redis.get("jetlag_xml"))

"""
根据游戏、平台获取对应时差
"""
def get_jetlag(param):

    # 直接读取xml文件(暂时不用这种方式)
    # try:
    #     tree = ET.parse("/data/etl_manage/conf/game_platform_jetlag.xml")
    #     #得到根节点
    #     root = tree.getroot()
    # except Exception as exc:
    #     print "Error: cannot parse file: game_platform_jetlag.xml."
    #     sys.exit(1)
    try:
        root = ET.fromstring(get_xml_from_redis())
        game = param['game']
        platform = param['platform']
        result = "0"

        for game_list in root.findall('game'):
            game_name = game_list.get('name')
            platform_list = game_list.findall('platform')
            if game_name == game:
                for platforms in platform_list:
                    if platforms.get("name") == platform:
                        result = platforms.get("jetlag")
                        break
        return result
    except Exception as exc:
        print(exc)
        sys.exit(1)

"""
根据游戏获取其存在时差的平台
"""
def get_platform(param):

    # 直接读取配xml文件(暂时不用这种方式)
    # try:
    #     tree = ET.parse("/data/etl_manage/conf/game_platform_jetlag.xml")
    #     #得到根节点
    #     root = tree.getroot()
    # except Exception as exc:
    #     print "Error: cannot parse file: game_platform_jetlag.xml."
    #     sys.exit(1)

    try:
        root = ET.fromstring(get_xml_from_redis())
        game = param['game']
        tmp = ""
        for game_list in root.findall('game'):
            game_name = game_list.get('name')
            platform_list = game_list.findall('platform')
            if game_name == game:
                for platform in platform_list:
                    tmp += str(platform.get("name")) + "','"
        result = "'" + tmp + "'"
        if result == "''":
            result = "'dev'"

        return result
    except Exception as exc:
        print(exc)
        sys.exit(1)

"""
获取全部游戏、平台、时差信息
"""
def get_game_platform():

    # 直接读取配xml文件(暂时不用这种方式)
    # try:
    #     tree = ET.parse("/data/etl_manage/conf/game_platform_jetlag.xml")
    #     #得到根节点
    #     root = tree.getroot()
    # except Exception as exc:
    #     print "Error: cannot parse file: game_platform_jetlag.xml."
    #     sys.exit(1)
    try:
        root = ET.fromstring(get_xml_from_redis())
        result = []
        for game_list in root.findall('game'):
            platform_list = game_list.findall('platform')
            for platform in platform_list:
                tmp = {'game': game_list.get('name'), 'platform': platform.get("name"), 'jetlag': platform.get("jetlag")}
                result.append(tmp)
        return result
    except Exception as exc:
        print(exc)
        sys.exit(1)

def main(argv):
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hg:p:', ['help', 'game=', 'platform='])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    VARS = {'game': None, 'platform': None}
    for opt, value in opts:

        if opt in ('-h', '--help'):
            print "-g, --game 游戏"
            print "-p, --platform 平台"
            sys.exit()

        if opt in ('-g', '--gme'):
            VARS['game'] = value
        elif opt in ('-p', '--platform'):
            VARS['platform'] = value

    if VARS['game'] is not None and VARS['platform'] is not None:
        print get_jetlag(VARS)
    elif VARS['game'] is not None and VARS['platform'] is None:
        VARS.pop('platform')
        print get_platform(VARS)
    elif VARS['game'] is None and VARS['platform'] is None:
        print get_game_platform()

if __name__ == '__main__':
    main(sys.argv)