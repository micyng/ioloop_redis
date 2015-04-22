#coding:utf-8

from __future__ import absolute_import

from tornado.ioloop import IOLoop
from tornado.concurrent import TracebackFuture
from tornado.iostream import IOStream
from tornado.stack_context import NullContext
import socket
from .redis_resp import decode_resp_ondemand
from .redis_encode import chain_select_cmd, _encode_req
from collections import deque
from util.convert import resolve_redis_url


_RESP_FUTURE = 'future'
RESP_ERR = 'err'
RESP_RESULT = 'r'


def _chain_cmds(trans, cmds):
    """对单条指令和pipe均支持

    :param trans: 是否使用事务
    :param cmds: 已通过相关函数编码后的字符串
    """
    if not isinstance(trans, bool):
        raise ValueError('trans not bool, but %s' % type(trans))

    count = 0
    if trans:
        yield count, _encode_req('MULTI')
    for _ in cmds:
        if not isinstance(_, str):
            raise ValueError('cmd not str: %s' % _)
        count += 1
        yield count, _
    if trans:
        yield count, _encode_req('EXEC')


class AsyncRedis(object):
    """
    一个redis地址对应一个AsyncRedis对象
    维护一个RedisConnection对象
    """
    def __init__(self, redis_uri=None, redis_tuple=None):
        if redis_uri:
            host, port, db, self.__pwd = resolve_redis_url(redis_uri)
            self.__redis_tuple = host, port, db
        elif redis_tuple:
            assert 4 == len(redis_tuple)
            self.__redis_tuple = redis_tuple[:3]
            self.__pwd = redis_tuple[-1]
        self.__conn = None

    def invoke(self, iter_redis_cmds, **kwargs):
        """异步调用redis相关接口

        :param iter_redis_cmds: 多条redis指令
        :param kwargs: 用于设置事务开关等
        """
        #如不包含事务参数，则默认开启；否则按设置执行
        active_trans = kwargs.get('active_trans')
        if active_trans is None:
            active_trans = True

        cmd_count = 0
        temp_buf = []
        for i, redis_command in _chain_cmds(active_trans, iter_redis_cmds):
            cmd_count = i
            temp_buf.append(redis_command)

        redis_stream = ''.join(temp_buf)
        del temp_buf
        future = TracebackFuture()

        def handle_resp(resp):
            f = resp.get(_RESP_FUTURE) or future
            err = resp.get(RESP_ERR)
            result = resp.get(RESP_RESULT)

            if err:
                f.set_exception(err)
            else:
                f.set_result(result)

        with NullContext():
            if self.__conn is None or not self.__conn.con_ok():
                self.__conn = _RedisConnection(handle_resp, self.__redis_tuple, self.__pwd)
                self.__conn.connect(future)
            self.__conn.write(redis_stream, future, active_trans, cmd_count)
        return future


class _RedisConnection(object):
    def __init__(self, final_callback, redis_tuple, redis_pwd):
        """
        :param final_callback: resp赋值时调用
        :param redis_tuple: (ip, port, db)
        :param redis_pwd: redis密码
        """
        self.__io_loop = IOLoop.instance()
        self.__resp_cb = final_callback
        self.__stream = None
        #redis应答解析remain
        self.__recv_buf = ''
        self.__redis_tuple = redis_tuple
        self.__redis_pwd = redis_pwd
        #redis指令上下文, connect指令个数(AUTH, SELECT .etc)，trans，cmd_count
        self.__cmd_env = deque()
        self.__cache_before_connect = []
        self.__connected = False

    def con_ok(self):
        """
        连接对象是否ok
        :return:
        """
        return self.__connected

    def connect(self, init_future):
        """
        connect指令包括：AUTH, SELECT
        :param init_future: 第一个future对象
        """
        #future, connect_count, transaction, cmd_count
        self.__cmd_env.append((init_future, 1 + int(bool(self.__redis_pwd)), False, 0))
        self.__stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0),
                                 io_loop=self.__io_loop)
        self.__stream.set_close_callback(self.__on_close)
        self.__stream.connect(self.__redis_tuple[:2], self.__on_connect)

    def __on_connect(self):
        """连接，只需要发送初始cmd即可
        """
        self.__connected = True
        self.__stream.set_nodelay(True)
        self.__stream.read_until_close(self.__last_closd_recv, self.__on_resp)
        self.__stream.write(chain_select_cmd(self.__redis_pwd, self.__redis_tuple[-1]))
        for x in self.__cache_before_connect:
            self.__stream.write(x)
        self.__cache_before_connect = []

    def write(self, buf, new_future, active_trans, cmd_count):
        """
        :param new_future: 由于闭包的影响，在resp回调函数中会保存上一次的future对象，该对象必须得到更新
        :param active_trans: 事务是否激活
        :param cmd_count: 指令个数
        """
        self.__cmd_env.append((new_future, 0, active_trans, cmd_count))
        if not self.__connected:
            self.__cache_before_connect.append(buf)
            return
        self.__stream.write(buf)

    def __last_closd_recv(self, data):
        """
        socket关闭时最后几个字节
        """
        if not data:
            return
        self.__on_resp(data)

    def __on_resp(self, recv):
        """
        :param recv: 收到的buf
        """
        recv = ''.join((self.__recv_buf, recv))

        idx = 0
        for future, connect, trans, cmd in self.__cmd_env:
            ok, payload, recv = decode_resp_ondemand(recv, connect, trans, cmd)
            if not ok:
                break

            idx += 1
            if not connect:
                self.__run_callback({_RESP_FUTURE: future, RESP_RESULT: payload})

        self.__recv_buf = recv
        for _ in xrange(idx):
            self.__cmd_env.popleft()

    def __run_callback(self, resp):
        if self.__resp_cb is None:
            return
        self.__io_loop.add_callback(self.__resp_cb, resp)

    def __on_close(self):
        self.__connected = False
        while len(self.__cmd_env) > 0:
            self.__run_callback({_RESP_FUTURE: self.__cmd_env.popleft(), RESP_RESULT: 0})
        self.__cmd_env.clear()