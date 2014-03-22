ioloop_redis
============

another async redis lib for tornado, short and intuitive

Sample
------

        from __future__ import absolute_import
        import time
        from tornado import httpserver
        from tornado.web import Application, RequestHandler
        from tornado.gen import coroutine
        from redis_client import *


        _queue = AsyncRedis('redis://localhost:6379/1')


        class HelloworldHandler(RequestHandler):
            @coroutine
            def get(self):
                next_cmd = yield _dev_queue.invoke(redis_lpop('key'))
                #do sth.
                self.write(next_cmd)
                self.finish()


        def main():
            app = Application([
                ('/', HelloworldHandler),
            ])

            server = httpserver.HTTPServer(app)
            server.listen(8000)
            IOLoop.instance().start()


        if __name__ == "__main__":
            main()