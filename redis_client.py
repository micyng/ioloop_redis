#coding:utf-8

"""
进入队列
封装针对tornado的异步redis库
"""

from __future__ import absolute_import

from tornado.ioloop import IOLoop
from tornado.concurrent import TracebackFuture
from tornado.iostream import IOStream
from tornado.stack_context import ExceptionStackContext, NullContext
from urlparse import urlparse
from itertools import imap
import socket
from .redis_resp import decode_resp_ondemand
from collections import deque


RESP_ERR = 'err'
_RESP_FUTURE = 'future'
RESP_RESULT = 'r'

_SYM_STAR = '*'
_SYM_DOLLAR = '$'
_SYM_CRLF = '\r\n'
_SYM_EMPTY = ''


def _parse_redis_url(url):
    """解析redis字符串
    :param url: redis://<ip>:<port>/db
    """
    url = urlparse(url)

    ip, port = url.netloc.split(':')
    port = int(port)

    db = int(url.path[1:])
    return ip, port, db


def __encode(value, encoding='utf-8', encoding_errors='strict'):
    """Return a bytestring representation of the value
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, float):
        return repr(value)
    if not isinstance(value, str):
        return str(value)
    if isinstance(value, unicode):
        return value.encode(encoding, encoding_errors)
    return value


def _encode_req(*args):
    """编码请求格式, 从Redis库中摘来
    """
    args_output = _SYM_EMPTY.join(
        [_SYM_EMPTY.join((_SYM_DOLLAR, str(len(k)), _SYM_CRLF, k, _SYM_CRLF)) for k in imap(__encode, args)])
    return _SYM_EMPTY.join((_SYM_STAR, str(len(args)), _SYM_CRLF, args_output))


def redis_pub(ch, msg):
    assert ch and isinstance(ch, str)
    assert msg and isinstance(msg, str)

    return _encode_req('PUBLISH', ch, msg)


def redis_hget(name, key):
    assert name and isinstance(name, str)
    assert key and isinstance(key, str)

    return _encode_req('HGET', name, key)


def redis_hset(name, key):
    return _encode_req('HSET', name, key)


def redis_get(key):
    assert key and isinstance(key, str)

    return _encode_req('GET', key)


def redis_set(key, value):
    return _encode_req('SET', key, value)


def redis_incre(key):
    return _encode_req('INCR', key)


def redis_incre_by(key, value):
    return _encode_req('INCRBY', key, value)


def redis_incre_byfloat(key, f):
    return _encode_req('INCRBYFLOAT', key, f)


def redis_lpop(key):
    """从列表中弹出首元素

    :param key: 键值
    """
    return _encode_req('LPOP', key)


def chain_select_cmd(db, cmd):
    """
    选择库的同时发送指令，作为一个pipe
    """
    assert isinstance(db, int) and 0 <= db <= 15
    assert cmd and isinstance(cmd, str)

    return ''.join((_encode_req('SELECT', db), cmd))


def _chain_cmds(trans, *args):
    """对单条指令和pipe均支持

    :param trans: 是否使用事务
    :param args: 已通过相关函数编码后的字符串
    """
    assert isinstance(trans, bool)

    cmds = []
    if trans:
        cmds.append(_encode_req('MULTI'))
    for _ in args:
        assert isinstance(_, str)
        cmds.append(_)
    if trans:
        cmds.append(_encode_req('EXEC'))

    cmds = ''.join(cmds)
    return cmds


class AsyncRedis(object):
    """
    一个redis地址对应一个AsyncRedis对象
    维护一个RedisConnection对象
    """

    def __init__(self, redis_uri):
        """
        :param redis_uri: redis://<ip>:<port>/<db>
        """
        self.__io_loop = IOLoop.instance()
        self.__uri = _parse_redis_url(redis_uri)
        self.__conn = None

    def invoke(self, *args):
        """异步调用redis相关接口

        :param args: 多条redis指令
        """
        #开启multi
        active_trans = True

        write_buf = _chain_cmds(active_trans, *args)
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
            if self.__conn is None:
                self.__conn = _RedisConnection(self.__io_loop, write_buf, handle_resp, self.__uri)
                self.__conn.connect(future, self.__uri, active_trans, len(args))
            else:
                self.__conn.write(write_buf, future, False, active_trans, len(args))
        return future


class _RedisConnection(object):
    def __init__(self, io_loop, init_buf, final_callback, redis_tuple):
        """
        :param io_loop: 你懂的
        :param init_buf: 第一次写入
        :param final_callback: resp赋值时调用
        :param redis_tuple: (ip, port, db)
        """
        self.__io_loop = io_loop
        self.__final_cb = final_callback
        self.__stream = None
        #redis应答解析remain
        self.__recv_buf = ''
        self.__init_buf = chain_select_cmd(redis_tuple[2], init_buf or '')
        self.__connected = False
        #redis指令上下文, 是否包含select，trans，cmd_count
        self.__cmd_env = deque()
        self.__written = False

    def connect(self, init_future, redis_tuple, active_trans, cmd_count):
        """
        :param init_future: 第一个future对象
        :param redis_tuple: (ip, port, db)
        :param active_trans: 事务是否激活
        :param cmd_count: 指令个数
        """
        if self.__stream is not None:
            return
        #future, include_select, transaction, cmd_count
        self.__cmd_env.append((init_future, True, active_trans, cmd_count))
        with ExceptionStackContext(self.__handle_ex):
            self.__stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0),
                                     io_loop=self.__io_loop)
            self.__stream.connect(redis_tuple[:2], self.__on_connect)

    def write(self, write_buf, new_future, include_select, active_trans, cmd_count, by_connect=False):
        """
        :param new_future: 由于闭包的影响，在resp回调函数中会保存上一次的future对象，该对象必须得到更新
        :param include_select: 是否包含SELECT指令
        :param active_trans: 事务是否激活
        :param cmd_count: 指令个数
        """
        if by_connect:
            self.__stream.write(self.__init_buf)
            self.__init_buf = None
            return

        self.__cmd_env.append((new_future, include_select, active_trans, cmd_count))
        if not self.__connected:
            self.__init_buf = ''.join((self.__init_buf, write_buf))
            return

        if self.__init_buf:
            write_buf = ''.join((self.__init_buf, write_buf))

        self.__stream.write(write_buf)
        self.__init_buf = None

    def __on_connect(self):
        """连接，只需要发送初始cmd即可
        """
        self.__connected = True
        self.__stream.set_nodelay(True)
        self.write(None, None, None, None, None, True)
        self.__stream.read_until_close(None, self.__on_resp)

    def __on_resp(self, recv):
        """
        :param recv: 收到的buf
        """
        recv = ''.join((self.__recv_buf, recv))

        idx = 0
        for future, select, trans, count in self.__cmd_env:
            ok, payload, recv = decode_resp_ondemand(recv, select, trans, count)
            if not ok:
                break

            idx += 1
            if 1 == count:
                payload = payload[0]
            self.__run_callback({_RESP_FUTURE: future, RESP_RESULT: payload})

        self.__recv_buf = recv
        for _ in xrange(idx):
            self.__cmd_env.popleft()

    def __run_callback(self, resp):
        if self.__final_cb is None:
            return

        self.__io_loop.add_callback(self.__final_cb, resp)

    def __handle_ex(self, typ, value, tb):
        """
        :param typ: 异常类型
        """
        if self.__final_cb:
            self.__run_callback({RESP_ERR: value})
            return True
        return False