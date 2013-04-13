#coding=utf8
from itertools import chain
import socket
import sys
import os
import socket
import functools

class SSDBError(Exception):
	def __init__(self, reason, *args):
		super(SSDBError, self).__init__(reason, *args)
		self.reason  = reason
		self.message = ' '.join(args)

class Connection(object):
	def __init__(self, host='127.0.0.1', port=8888):
		self.pid = os.getpid()
		self.host = host
		self.port = port
		self.connect()

	def connect(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.connect((self.host, self.port))
		self.fp = self.socket.makefile('rb')

	def close(self):
		self.fp.close()
		self.socket.close()

	def reconnect(self):
		try:
			self.close()
		except:
			pass
		finally:
			self.connect()

class ConnectionPool(object):
	"""Generic connection pool"""
	def __init__(self, connection_class=Connection, max_connections=None, **connection_kwargs):
		self.pid = os.getpid()
		self.connection_class = connection_class
		self.connection_kwargs = connection_kwargs
		self.max_connections = max_connections or 2 ** 31
		self._created_connections = 0
		self._available_connections = []
		self._in_use_connections = set()

	def _checkpid(self):
		"""current process can't operate other process connection pool"""
		if self.pid != os.getpid():
			self.disconnect()
			self.__init__(self.connection_class, self.max_connections,**self.connection_kwargs)

	def get_connection(self):
		"""Get a connection from the pool"""
		self._checkpid()
		try:
			connection = self._available_connections.pop()
		except IndexError:
			connection = self.make_connection()
		self._in_use_connections.add(connection)
		return connection

	def make_connection(self):
		"""Create a new connection"""
		if self._created_connections >= self.max_connections:
			raise ConnectionError("Too many connections")
		self._created_connections += 1
		return self.connection_class(**self.connection_kwargs)

	def release(self, connection):
		"""Releases the connection back to the pool"""
		self._checkpid()
		if connection.pid == self.pid:
			self._in_use_connections.remove(connection)
			self._available_connections.append(connection)

	def disconnect(self):
		"""Disconnects all connections in the pool"""
		all_conns = chain(self._available_connections, self._in_use_connections)
		for connection in all_conns:
			connection.disconnect()

class Client(object):
	def __init__(self, host='127.0.0.1', port=8888):
		self.connection_pool = ConnectionPool(host=host, port=port)

	def close(self):
		self.connection_pool.disconnect()

	def _send(self, cmd, *args):
		if cmd == 'delete':
			cmd = 'del'
		args = (cmd, ) + args
		if isinstance(args[-1], int):
			args = args[:-1] + (str(args[-1]), )
		buf = ''.join('%d\n%s\n' % (len(i), i) for i in args) + '\n'

		connection = self.connection_pool.get_connection()
		try:
			connection.socket.sendall(buf)
		except:
			connection.reconnect()
			connection.socket.sendall(buf)

		return_list = 'keys' in cmd or 'scan' in cmd or 'list' in cmd
		try:
			return self._recv(return_list, connection)
		except Exception as e:
			raise e
		finally:
			self.connection_pool.release(connection)

	def _recv(self, return_list=False, connection=None):
		ret = []
		while True:
			line = connection.fp.readline().rstrip('\n')
			if not line:
				break
			data = connection.fp.read(int(line))
			connection.fp.read(1) #discard '\n'
			ret.append(data)
		if ret[0] == 'not_found':
			return None
		if ret[0] == 'ok':
			ret = ret[1:]
			if return_list:
				return ret
			if not ret:
				return None
			if len(ret) == 1:
				return ret[0]
		raise SSDBError(*ret)

	def __getattr__(self, cmd):
		if cmd in self.__dict__:
			return self.__dict__[cmd]
		elif cmd in self.__class__.__dict__:
			return self.__class__.__dict__[cmd]
		ret = self.__dict__[cmd] = functools.partial(self._send, cmd)
		return ret

if __name__ == '__main__':
	c = Client()
	print c.set('key', 'value')
	print c.get('key')
	import string
	for i in string.ascii_letters:
		c.incr(i)
	print c.keys('a', 'z', 1)
	print c.keys('a', 'z', 10)
	print c.get('z')

	from datetime import datetime
	_start = datetime.now()
	for i in xrange(100000):
		c.incr('z')
		c.keys('a', 'a', 1)
		c.keys('a', 'z', 10)
		c.get('z')
	print datetime.now() - _start