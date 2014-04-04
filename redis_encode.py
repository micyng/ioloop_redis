#coding:utf-8


from itertools import imap


_SYM_STAR = '*'
_SYM_DOLLAR = '$'
_SYM_CRLF = '\r\n'
_SYM_EMPTY = ''


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


def redis_auth(password):
    assert password and isinstance(password, str)
    return _encode_req('AUTH', password)


def redis_pub(ch, msg):
    assert ch and isinstance(ch, str)
    assert msg and isinstance(msg, str)

    return _encode_req('PUBLISH', ch, msg)


def redis_hget(name, key):
    assert name and isinstance(name, str)
    assert key and isinstance(key, str)

    return _encode_req('HGET', name, key)


def redis_hset(name, key, value):
    assert name and isinstance(name, str)
    assert key and isinstance(key, str)
    assert isinstance(value, str)

    return _encode_req('HSET', name, key, value)


def redis_hsetnx(key, field, value):
    """
    将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在
    """
    assert key and isinstance(key, str)
    assert field and isinstance(field, str)
    assert value is not None

    return _encode_req('HSETNX', key, field, value)


def redis_get(key):
    assert key and isinstance(key, str)

    return _encode_req('GET', key)


def redis_set(key, value):
    assert key and isinstance(key, str)
    assert isinstance(value, str)

    return _encode_req('SET', key, value)


def redis_hkeys(key):
    assert key and isinstance(key, str)

    return _encode_req('HKEYS', key)


def redis_hexists(key, field):
    """
    查看哈希表 key 中，给定域 field 是否存在
    """
    assert key and isinstance(key, str)
    assert field and isinstance(field, str)

    return _encode_req('HEXISTS', key, field)


def redis_setnx(key, value):
    """
    将 key 的值设为 value ，当且仅当 key 不存在
    """
    assert key and isinstance(key, str)
    assert value is not None

    return _encode_req('SETNX', key, value)


def redis_sadd(key, member):
    """添加hashset成员
    """
    assert key and isinstance(key, str)
    assert isinstance(member, str)

    return _encode_req('SADD', key, member)


def redis_scard(key):
    """返回hashset总数
    """
    assert key and isinstance(key, str)

    return _encode_req('SCARD', key)


def redis_incre(key):
    assert key and isinstance(key, str)

    return _encode_req('INCR', key)


def redis_incre_by(key, value):
    assert key and isinstance(key, str)
    assert isinstance(value, int)

    return _encode_req('INCRBY', key, value)


def redis_incre_byfloat(key, f):
    assert key and isinstance(key, str)
    assert isinstance(f, float)

    return _encode_req('INCRBYFLOAT', key, f)


def redis_sismember(key, member):
    """
    判断 member 元素是否集合 key 的成员
    """
    assert key and isinstance(key, str)
    assert member and isinstance(member, str)

    return _encode_req('SISMEMBER', key, member)


def redis_smembers(key):
    assert key and isinstance(key, str)

    return _encode_req('SMEMBERS', key)


def redis_lpop(key):
    """从列表中弹出首元素

    :param key: 键值
    """
    assert key and isinstance(key, str)

    return _encode_req('LPOP', key)


def redis_lrange(key, begin, end):
    """
    :param begin: 开始索引
    :param end: 结束索引
    """
    assert key and isinstance(key, str)
    assert isinstance(begin, int)
    assert isinstance(end, int)

    return _encode_req('LRANGE', key, begin, end)


def redis_rpush(key, value):
    assert key and isinstance(key, str)
    assert value and isinstance(value, str)

    return _encode_req('RPUSH', key, value)


def chain_select_cmd(db, cmd):
    """
    选择库的同时发送指令，作为一个pipe
    """
    assert isinstance(db, int) and 0 <= db <= 15
    assert cmd and isinstance(cmd, str)

    return ''.join((_encode_req('SELECT', db), cmd))