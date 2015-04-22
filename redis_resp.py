#coding:utf-8

import re

#应答结束字符
_END_CRLF = '\r\n'
#bulk获取字符串长度正则
_bulk_len_pat = re.compile(r'^\$(-?\d+?)\r\n')
#整数
_int_pat = re.compile(r'^:(-?\d+?)\r\n')
#错误
_err_pat = re.compile(r'^-([^\r\n]+?)\r\n')
_single_pat = re.compile(r'^\+([^\r\n]+?)\r\n')
#批应答
_batch_pat = re.compile(r'\*(?P<count>-?\d+?)\r\n')


def _single_line(s):
    m = _single_pat.search(s)
    if not m:
        return False, None, None
    return True, m.groups()[0], s[m.end():]


def _err(s):
    m = _err_pat.search(s)
    if not m:
        return False, None, None
    return True, m.groups()[0], s[m.end():]


def _int(s):
    """
    @return 整型，剩余字符串
    """
    m = _int_pat.search(s)
    if not m:
        return False, None, None
    return True, int(m.groups()[0]), s[m.end():]


def _bulk(s):
    """
    @return 档次解析的body，剩余字符串
    """
    m = _bulk_len_pat.search(s)
    if not m:
        return False, None, None

    body_len = int(m.groups()[0])
    #-1 --> None
    #0 --> ''
    #>0 --> '...'
    if body_len < -1:
        return False, None, None

    if -1 == body_len:
        return True, None, s[m.end():]

    match_len = m.end()
    #<match>, body, \r\n
    expect_len = match_len + body_len + 2

    if len(s) < expect_len:
        return False, None, None

    if s[match_len + body_len: match_len + body_len + 2] != _END_CRLF:
        return False, None, None

    return True, s[match_len: match_len + body_len], s[match_len + body_len + 2:]


def _batch(s):
    """批量应答，和发送协议一致
    返回tuple

    :param s: 字符串
    """
    m = _batch_pat.search(s)
    if not m:
        return False, None, None

    count = int(m.groupdict().get('count'))
    if -1 == count:
        return True, None, s[m.end():]
    elif 0 == count:
        return True, (), s[m.end():]

    s = s[m.end():]
    ok, l, r = decode_redis_resp(s, batch_count=count)
    if not ok:
        return False, None, None

    return True, l, r


_map = {
    '+': _single_line,
    '-': _err,
    ':': _int,
    '$': _bulk,
    '*': _batch,
}


def decode_redis_resp(s, batch_count=None):
    """一次性解析redis应答

    :param s: 必须为utf8编码的字符串
    :param batch_count: batch指令总数上限

    对于开启MULTI的指令（和是否pipeline无关），例如:

    MULTI
    LPOP hello
    LPOP world
    EXEC

    应答类似: +OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n$-1\r\n$-1\r\n
    在pipe中append了几次，就会有相应的几个QUEUED，以及最终的应答为tuple

    'OK', 'QUEUED', 'QUEUED', (None, None)

    如果未开启MULTI,，则应答：
    $-1\r\n$-1\r\n --> None, None

    batch_count用于batch指令个数
    """
    if s is None:
        return False, None, ''
    if isinstance(s, str):
        if not s:
            return True, None, ''
        if len(s) < 3:
            return False, None, s

    remain = s
    result = []
    #according to http://docs.python.org/2/faq/design.html#how-are-lists-implemented
    #python list的len操作时间复杂度为O(1)

    while 1:
        if remain is not None and 0 == len(remain):
            break

        if batch_count is not None and len(result) >= batch_count:
            return True, tuple(result), remain

        head = remain[0]
        if head not in _map:
            break

        ok, body, _ = _map.get(head)(remain)
        if ok:
            remain = _
        else:
            if not result:
                return False, None, remain
            else:
                break

        if batch_count is None or len(result) < batch_count:
            result.append(body)

    return True, tuple(result), remain


#+OK\r\n
CONNECT_RESP_LEN = 5


def decode_resp_ondemand(buf, connect_count, trans_active, cmd_count):
    """用于应答，按需解析buf

    :param buf: 应答buf
    :param connect_count: AUTH, ECHO, PING, SELECT, QUIT等指令时+1
    :param trans_active: 是否启用事务
    :param cmd_count: 请求命令个数
    """
    if not buf or not isinstance(buf, str):
        return False, None, buf

    if not (isinstance(connect_count, int) and connect_count >= 0):
        raise ValueError('connect invalid: {0}'.format(connect_count))

    #期待最小长度(+OK\r\n)，当connect_count为0时，为0
    #正常情况下AUTH,SELECT返回+OK\r\n，此处将ECHO,PING作为普通命令看待
    expect_minlen = CONNECT_RESP_LEN * connect_count
    if trans_active:
        #+OK\r\n
        expect_minlen += 5
        #+QUEUED\r\n
        expect_minlen += 9 * cmd_count

    if len(buf) < expect_minlen:
        return False, None, buf

    cursor = 0
    #需判定是否内容也符合
    for i in xrange(connect_count):
        end = (i + 1) * CONNECT_RESP_LEN
        ok, p, remain = decode_redis_resp(buf[i * CONNECT_RESP_LEN: end])
        #判定是否为OK
        if not ok:
            return False, None, buf
        if 1 != len(p) or 'OK' != p[0]:
            return False, None, buf
        cursor = end

    #TODO: 需排除以+前缀的命令字符串
    if trans_active:
        #需要排除*1r\n这种提示符
        s = buf[cursor:]
        #+1： MULTI指令应答为+OK\r\n+QUEUED\r\n * n
        ok, p, remain = decode_redis_resp(s, 1 + cmd_count)
        if not ok:
            return False, None, buf
        if len(p) != 1 + cmd_count:
            return False, None, buf
        if 'OK' != p[0]:
            return False, None, buf
        if ('QUEUED',) * cmd_count != p[1:]:
            return False, None, buf
        cursor = expect_minlen

    if not cmd_count:
        if connect_count:
            #连接态
            return True, None, buf[cursor:]
        if not trans_active:
            return False, None, buf[cursor:]

        #对于有连接指令或者事务启用的情况，ok为False则早已返回
        #木有cmd_count
        ok, p, remain = decode_redis_resp(buf[cursor:], 1)
        if not ok:
            del remain
            return False, None, buf[cursor:]
        if isinstance(p, (list, tuple)) and 1 == len(p):
            p = p[0]
        return ok, p, remain

    ok, p, remain = decode_redis_resp(buf[cursor:], cmd_count)
    if not ok:
        del remain
        return False, None, buf[cursor:]

    assert isinstance(p, (list, tuple))
    if trans_active:
        p = p[0]
        
    if len(p) != cmd_count:
        del remain
        return False, None, buf[cursor:]

    #从业务调用简单考虑，当cmd_count为1时直接返回列表第一个元素
    return True, p[0] if 1 == cmd_count else p, remain