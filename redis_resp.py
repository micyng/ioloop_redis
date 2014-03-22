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
_batch_pat = re.compile(r'\*(?P<count>\d+?)\r\n')


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
    if 0 == count:
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
    if not s or not isinstance(s, str) or len(s) < 3:
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


def decode_resp_ondemand(buf, has_select, trans_active, cmd_count):
    """用于应答，按需解析buf

    :param buf: 应答buf
    :param has_select: 是否包含select db指令
    :param trans_active: 是否启用事务
    :param cmd_count: 请求命令个数
    """
    
    if not buf or not isinstance(buf, str):
        return False, None, buf

    #期待最小长度
    #+OK\r\n
    expect_minlen = 5 if has_select else 0
    if trans_active:
        #+OK\r\n
        expect_minlen += 5
        #+QUEUED\r\n
        expect_minlen += 9 * cmd_count

    if len(buf) < expect_minlen:
        return False, None, buf

    cursor = 0
    #需判定是否内容也符合
    if has_select:
        ok, p, remain = decode_redis_resp(buf[:5])
        #判定是否为OK
        if not ok:
            return False, None, buf
        if 1 != len(p) or 'OK' != p[0]:
            return False, None, buf
        cursor = 5
        
    #TODO: 需排除以+前缀的命令字符串
    if trans_active:
        #需要排除*1r\n这种提示符
        s = buf[cursor:]
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

    ok, p, remain = decode_redis_resp(buf[cursor:], cmd_count)
    if not ok:
        return False, None, remain
        
    if len(p) != cmd_count:
        return False, None, remain
        
    return True, p[0] if 1 == cmd_count else p, remain
    
    
from collections import deque


def build_sample():
    s = ['+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n']
    tot = 50000
    cmd_env = deque()
    cmd_env.append((True, True, 1))
    
    for _ in xrange(tot):
        cmd_env.append((False, True, 1))
        s.append('+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n')
        
    return cmd_env, ''.join(s)
    

env, recv = build_sample()


def main():
    global env
    global recv    
    
    idx = 0
    for select, trans, count in env:
        ok, payload, recv = decode_resp_ondemand(recv, select, trans, count)
        if not ok:
            break

        idx += 1
        if 1 == count:
            payload = payload[0]

    print 'recv:%r' % recv
    for _ in xrange(idx):
        env.popleft()
        
        
#该函数性能不是问题
"""
         620076 function calls (610075 primitive calls) in 0.985 seconds

   Ordered by: standard name

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
        1    0.000    0.000    0.985    0.985 <string>:1(<module>)
30004/20003    0.140    0.000    0.821    0.000 redis_resp.py:104(decode_redis_resp)
    10001    0.109    0.000    0.938    0.000 redis_resp.py:161(decode_resp_ondemand)
    20003    0.196    0.000    0.229    0.000 redis_resp.py:18(_single_line)
        1    0.045    0.045    0.985    0.985 redis_resp.py:240(main)
    10001    0.168    0.000    0.185    0.000 redis_resp.py:42(_bulk)
    10001    0.180    0.000    0.446    0.000 redis_resp.py:73(_batch)
    40005    0.008    0.000    0.008    0.000 {isinstance}
   280031    0.052    0.000    0.052    0.000 {len}
    40005    0.010    0.000    0.010    0.000 {method 'append' of 'list' objects}
        1    0.000    0.000    0.000    0.000 {method 'disable' of '_lsprof.Profiler' objects}
    50006    0.011    0.000    0.011    0.000 {method 'get' of 'dict' objects}
    40005    0.009    0.000    0.009    0.000 {method 'group' of '_sre.SRE_Match' objects}
    10001    0.009    0.000    0.009    0.000 {method 'groupdict' of '_sre.SRE_Match' objects}
    30004    0.009    0.000    0.009    0.000 {method 'groups' of '_sre.SRE_Match' objects}
    10001    0.002    0.000    0.002    0.000 {method 'popleft' of 'collections.deque' objects}
    40005    0.036    0.000    0.036    0.000 {method 'search' of '_sre.SRE_Pattern' objects}
    
"""    
        
import cProfile

if __name__ == '__main__':
    cProfile.run('main()')
    
    
    
