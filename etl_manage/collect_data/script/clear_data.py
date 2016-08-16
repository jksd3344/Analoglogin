#!/usr/bin/python

import os
import datetime

wait_clean = [
        '/data/log',
	
    ]

def clean(path):
    files = os.popen("find %s -mtime +3 -type f" % path).read()
    print '[%s] delete: \n%s' % (datetime.datetime.today().strftime('%Y-%m-%d'), files)
    os.system("rm -rf `find %s -mtime +3 -type f`" % path)

map(clean, wait_clean)
