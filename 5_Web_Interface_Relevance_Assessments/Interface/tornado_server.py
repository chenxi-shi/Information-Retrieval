import os
import re
import time
from math import ceil

import pymongo
from tornado import ioloop, web, autoreload, httpserver, escape
from tornado.escape import utf8
from tornado.options import define, options

from es_methods import *

define("port", default=8888, help="run on the given port", type=int)


class BaseHandler(web.RequestHandler):
	def get_current_user(self):
		return self.get_secure_cookie('username')


class LoginHandler(BaseHandler):
	def get(self):
		try:
			errormessage = self.get_argument("error")
		except:
			errormessage = ""
		self.render("login.html", errormessage=errormessage)

	def check_permission(self, username):
		if username in {'Chenxi', 'Ran', 'Yu'}:
			return True
		return False

	def post(self):
		username = self.get_argument("username", "")
		if isinstance(username, bytes):
			username = username.decode("utf-8")
		auth = self.check_permission(username)
		if auth:
			self.current_user = username
			self.set_secure_cookie("username", username)
			self.redirect(self.get_argument("next", "/"))
		else:
			error_msg = "?error=" + escape.url_escape("Login incorrect")
			self.redirect("/login" + error_msg)


class LogoutHandler(BaseHandler):
	def post(self):
		# if (self.get_argument("logout", None)):
		self.clear_cookie("username")
		self.redirect(self.get_argument("next", "/"))


class MainHandler(BaseHandler):
	def initialize(self):
		self.user_is_logged_in = False
		self._count = 200
		# self._fields = ["text"]
		self._query = ''

	@web.authenticated
	def get(self):
		if not self.current_user:
			self.redirect("/login")
			return
		if isinstance(self.current_user, bytes):
			self.current_user = self.current_user.decode('utf-8')
		username = escape.xhtml_escape(self.current_user)
		self.render("template.html", title="Welcome {}".format(username), hits=[],
		            search="Search")  # one page one render

	# TODO: 分页

	def post(self):
		# get result from elasticsearch
		self._query = self.get_argument('query')
		if self._query.strip() == 'costa concordia disaster and recovery':
			coll = self.application.db.page_text_table_152601
		elif self._query.strip() == 'South Korea ferry disaster':
			coll = self.application.db.page_text_table_152602
		elif self._query.strip() == 'Lampedusa migrant shipwreck':
			coll = self.application.db.page_text_table_152603
		else:
			coll = self.application.db.page_text_table
		self.took_time, self.hits = get_top_several(es, source_index, my_type, self._query, _count=self._count)
		if self.hits:
			self.parsed_hits = []
			# restore result into MongoDB
			for _hit in self.hits:
				self._res = coll.find_one({'url': _hit["_id"]})
				self._his_relev = "NOTHING"

				# TODO: change to better, for updata MongoDB
				if self._res:  # if record in MongoDB
					if isinstance(self.current_user, bytes):
						self.current_user = self.current_user.decode('utf-8')
					if self.current_user in self._res:
						if self.current_user in self._res:
							if self._res[self.current_user] == 0:
								self._his_relev = 'IRRELEVANT'
							elif self._res[self.current_user] == 1:
								self._his_relev = 'RELEVANT'
							elif self._res[self.current_user] == 2:
								self._his_relev = 'RELEVANT++'

					# TODO: Delete this line
					# self._res.update({"url": _hit["_id"], "title": _hit['fields']['title'][0], "text": _hit['highlight']['text'][0]})
					# coll.save(self._res)

				else:   # if no record in MongoDB

					coll.insert({"url": _hit["_id"], "title": _hit['fields']['title'][0], "text": _hit['highlight']['text'][0]})
					self._res = None

				self.parsed_hits.append((_hit["_id"], _hit['fields']['title'][0], _hit['highlight']['text'][0], self._his_relev))
			# render result urls to page
			self.render("search_results.html",
			            title="Result for \"{}\"".format(self._query),
			            hits=self.parsed_hits,
			            query=self._query.strip(),
			            took_time=self.took_time,
			            offset=0,
			            range=range,
			            ceil=ceil,
			            search="Search")  # one page one render
		else:
			self.render("no_results.html",
			            title="Result for \"{}\"".format(self._query),
			            took_time=self.took_time,
			            search="Search")  # one page one render

		# if self.get_argument('message') == 'Chenxi':
		self.user_is_logged_in = True

		if not self.user_is_logged_in:
			raise web.HTTPError(403)


class TextHandler(BaseHandler):
	def get(self):
		self.url = self.get_argument('url', None, True)
		self._query = self.get_argument('query', None, True)
		if self._query.strip() == 'costa concordia disaster and recovery':
			coll = self.application.db.page_text_table_152601
		elif self._query.strip() == 'South Korea ferry disaster':
			coll = self.application.db.page_text_table_152602
		elif self._query.strip() == 'Lampedusa migrant shipwreck':
			coll = self.application.db.page_text_table_152603
		else:
			coll = self.application.db.page_text_table
		self._res = coll.find_one({'url': self.url})
		self.current_date = time.strftime("%Y-%m-%d")
		if self._res:
			self._his_relev = 'Nothing'
			if isinstance(self.current_user, bytes):
				self.current_user = self.current_user.decode('utf-8')
			if self.current_user in self._res:
				if self._res[self.current_user] == 0:
					self._his_relev = 'irrelevant'
				elif self._res[self.current_user] == 1:
					self._his_relev = 'relevant'
				elif self._res[self.current_user] == 2:
					self._his_relev = 'relevant++'


			self.render("text.html",
			            url=self.url,
			            text=self._res["text"],
			            # render_string=self.render_string,
			            # linkify=escape.linkify,
			            query=self._query.strip(),
			            title=self._res["title"],
			            his_relev=self._his_relev)

			# self.write(self._res["text"])
		else:
			self.set_status(404)

	def post(self):
		self.url = self.get_argument('url', None, True)
		self._query = self.get_argument('query', None, True)
		if self._query.strip() == 'costa concordia disaster and recovery':
			coll = self.application.db.page_text_table_152601
		elif self._query.strip() == 'South Korea ferry disaster':
			coll = self.application.db.page_text_table_152602
		elif self._query.strip() == 'Lampedusa migrant shipwreck':
			coll = self.application.db.page_text_table_152603
		else:
			coll = self.application.db.page_text_table		# get result from elasticsearch
		self._relevant = self.get_argument('relevant', None, True)
		self.page_in_mongo = coll.find_one({'url': self.url})
		if isinstance(self.current_user, bytes):
			self.current_user = self.current_user.decode('utf-8')
		if self._relevant == '0':
			self.page_in_mongo[self.current_user] = 0
			coll.save(self.page_in_mongo)
			self.write('Thank you, {}, you choose irrelevant. Database updated.'.format(self.current_user))
		elif self._relevant == '1':
			self.page_in_mongo[self.current_user] = 1
			coll.save(self.page_in_mongo)
			self.write('Thank you, {}, you choose relevant. Database updated.'.format(self.current_user))
		elif self._relevant == '2':
			self.page_in_mongo[self.current_user] = 2
			coll.save(self.page_in_mongo)
			self.write('Thank you, {}, you choose relevant++. Database updated.'.format(self.current_user))
		elif self._relevant == 'empty':
			self.write('You didn\'t choose any answer.')
		else:
			self.write('Please input valid relevant info')

		# if self.get_argument('message') == 'Chenxi':
		self.user_is_logged_in = True

		if not self.user_is_logged_in:
			raise web.HTTPError(403)


class Application(web.Application):
	def __init__(self):
		settings = dict(
			static_path=os.path.join(os.path.dirname(__file__), "static"),  # tell where to find static dir
			template_path=os.path.join(os.path.dirname(__file__), 'templates'),
			debug=True,
			cookie_secret="61yUTzRANAGaYdkL5gEmChenxiYh7EQnp2XdTP1o/Vo=",
			login_url="/login",
		)
		handlers = [
			(r'/', MainHandler),
			(r'/text', TextHandler),
			(r"/login", LoginHandler),
			(r"/logout", LogoutHandler),
			(r"/(apple-touch-icon\.png)", web.StaticFileHandler, dict(path=settings['static_path']))
		]

		conn = pymongo.MongoClient('localhost', 27017)
		self.db = conn.page_text_db
		web.Application.__init__(self, handlers, **settings)


if __name__ == '__main__':
	# temp_dir = tempfile.mkdtemp(dir=os.path.join(os.path.dirname(__file__), "static"))
	options.parse_command_line()
	http_server = httpserver.HTTPServer(Application())
	http_server.listen(options.port)
	# TODO remove in prod
	autoreload.start()
	autoreload.watch('template.html es_method.py search_results.html')

	ioloop.IOLoop.instance().start()
