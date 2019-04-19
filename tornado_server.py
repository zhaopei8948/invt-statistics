from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from invt_statistics import app
http_server = HTTPServer(WSGIContainer(app))
http_server.listen(5006)
IOLoop.instance().start()
