#!/usr/bin/python
#coding=utf-8


import urllib2
import json
import urllib
import cookielib


class Analogin(object):

    def __init__(self):
        pass



    def login(self):
        data={
            "checkBrow":"",
            "pwdError":0,
            "userName":"liuxin03",
            "userPassword":"Jksd3344",
            "checkbox":"on"
        }

        post_data=urllib.urlencode(data)
        cj=cookielib.CookieJar()
        opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        headers = {"User-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"}
        req=urllib2.Request("http://oa.playcrab-inc.com/jsoa/CheckUser.jspx",post_data,headers)
        content=opener.open(req)
        content.read()


        pass



















