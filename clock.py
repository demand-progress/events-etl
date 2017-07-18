# encoding=utf8

from apscheduler.schedulers.blocking import BlockingScheduler
from etl.teaminternet import main as teaminternet

from rq import Queue
from worker import conn

sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=30)
def timed_for_teaminternet():
    print('Running Job for Team Internet')
    q = Queue(connection=conn)
    result = q.enqueue(teaminternet.queue, timeout=500)

sched.start()
