import redis
import sys
import json

from flask import current_app as app


class Cache:

	def __init__(self, host=None):
		host = host if host is not None else app.config.get('REDIS_URL')
		self.cache = redis.Redis(host=host)


	def put(self, key, data, sec=(60*60)):
		if sec == None:
			self.cache.set(key, data)
		else:
			self.cache.setex(key, data, sec)


	def put_json(self, key, data, sec=(60*60)):
		str_data = ""
		try:
			data = json.dumps(data)
		except:
			data = ""

		self.put(key, data, sec)


	def get(self, key):
		data = None
		try:
			data = self.cache.get(key)
		except:
			pass

		return '' if data is None else data.decode("utf-8")


	def get_json(self, key):
		data = None
		# data_json = {}
		try:
			data = self.cache.get(key)
		except:
			pass

		if data is not None:
			try:
				return json.loads(data)
			except:
				pass

		return {}


	def delete(self, key):
		self.cache.delete(key)
		

	def delete_all(self):
		self.cache.flushdb()

	
	def extend_expire(self, key, sec=(60*60)):
		self.cache.expire(key, sec)


	def list_key_matching(self, pattern=u'*'):
		return self.cache.keys(pattern)

