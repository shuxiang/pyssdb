import gevent
from gevent import monkey
monkey.patch_all()

from pyssdb_pool import Client as c1
from pyssdb import Client as c2

from datetime import datetime

#---------test ssdb client with connect pool
c = c1()
def call_c1():
	for i in xrange(10):
		c.incr('z')
		c.keys('a', 'a', 1)
		c.keys('a', 'z', 10)
		c.get('z')

_start = datetime.now()
for i in xrange(100):
	jobs = [gevent.spawn(call_c1) for i in xrange(100)]
	gevent.joinall(jobs)
print datetime.now() - _start
print c.connection_pool._created_connections

##---------test ssdb client without connect pool
# def call_c2():
# 	c = c2()
# 	for i in xrange(10):
# 		c.incr('z')
# 		c.keys('a', 'a', 1)
# 		c.keys('a', 'z', 10)
# 		c.get('z')

# _start = datetime.now()
# for i in xrange(100):
# 	jobs = [gevent.spawn(call_c2) for i in xrange(100)]
# 	gevent.joinall(jobs)
# print datetime.now() - _start
