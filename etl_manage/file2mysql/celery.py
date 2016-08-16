#!/usr/bin/python
#coding=utf-8

"""
auth:wuqichao
mail:wuqichao@playcrab.com
createtime:2015-9-17 10:00:00
usege: 用于hadoop下载文件任务操作

"""



from __future__ import absolute_import

from celery import Celery
from datetime import timedelta
from celery.schedules import crontab


from celery import bootsteps

class InfoStep(bootsteps.Step):

    def __init__(self, parent, **kwargs):
        # here we can prepare the Worker/Consumer object
        # in any way we want, set attribute defaults and so on.
        print('{0!r} is in init'.format(parent))

    def start(self, parent):
        # our step is started together with all other Worker/Consumer
        # bootsteps.
        print('{0!r} is starting'.format(parent))

    def stop(self, parent):
        # the Consumer calls stop every time the consumer is restarted
        # (i.e. connection is lost) and also at shutdown.  The Worker
        # will call stop at shutdown only.
        print('{0!r} is stopping'.format(parent))

    def shutdown(self, parent):
        # shutdown is called by the Consumer at shutdown, it's not
        # called by Worker.
        print('{0!r} is shutting down'.format(parent))



app = Celery('file2mysql',
             broker='redis://localhost:6379/0',
             backend='redis://localhost:6379/0',
             include=['file2mysql.tasks'])

app.steps['worker'].add(InfoStep)


# Optional configuration, see the application user guide.
app.conf.update(

    CELERY_TASK_RESULT_EXPIRES = 3600 , # celery任务执行结果的超时时间，我的任务都不需要返回结果,只需要正确执行就行
    CELERYD_CONCURRENCY = 40 , # celery worker的并发数 也是命令行-c指定的数目,事实上实践发现并不是worker也多越好,保证任务不堆积,加上一定新增任务的预留就可以
    CELERYD_PREFETCH_MULTIPLIER = 20 , # celery worker 每次去rabbitmq取任务的数量，我这里预取了4个慢慢执行,因为任务有长有短没有预取太多
    CELERYD_MAX_TASKS_PER_CHILD = 200 ,

    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['pickle', 'json', 'msgpack', 'yaml'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='json',


    CELERY_ROUTES = {
        'file2mysql.tasks.run_task': {'queue': 'file2mysql'},
    }


    # CELERYBEAT_SCHEDULE={
    #     "add":{
    #         "task":"hadoop.tasks.download",
    #         "schedule":timedelta(seconds=10), # 10s执行一次
    #         "args":(),
    #         'options': {'queue' : 'hadoop'} 
    #         },
    #     },

)

if __name__ == '__main__':
    app.start()
