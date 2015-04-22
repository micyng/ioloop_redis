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


def redis_hdel(key, field):
    assert key and isinstance(key, str)
    assert field and isinstance(field, str)

    return _encode_req('HDEL', key, field)


def redis_hget(name, key):
    assert name and isinstance(name, str)
    assert key and isinstance(key, str)

    return _encode_req('HGET', name, key)


def redis_hgetall(key):
    assert key and isinstance(key, str)
    return _encode_req('HGETALL', key)


def redis_hkeys(key):
    assert key and isinstance(key, str)
    return _encode_req('HKEYS', key)


def redis_hvals(key):
    assert key and isinstance(key, str)
    return _encode_req('HVALS', key)


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


def redis_getset(key, value):
    """
    将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
    当 key 存在但不是字符串类型时，返回一个错误。
    """
    assert key and isinstance(key, str)
    assert value is not None

    return _encode_req('GETSET', key, value)


def redis_delete(key):
    """
    删除给定的一个或多个 key
    """
    assert key and isinstance(key, str)
    return _encode_req('DEL', key)


def redis_exists(key):
    """
    存在键值
    :param key:
    :return:
    """
    assert key and isinstance(key, str)
    return _encode_req('EXISTS', key)


def redis_expire(key, seconds):
    """
    为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。

    :param seconds: 秒数
    """
    assert key and isinstance(key, str)
    assert isinstance(seconds, int) and seconds > 0

    return _encode_req('EXPIRE', key, seconds)


def redis_expireat(key, timestamp):
    """
    为给定 key 设置生存时间，当系统时间为timestamp时 key 过期，它会被自动删除。

    :param timestamp: key过期的时间戳
    """
    assert key and isinstance(key, str)
    return _encode_req('EXPIREAT', key, timestamp)


def redis_pexpire(key, milliseconds):
    """
    为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。

    :param milliseconds: 毫秒数
    """
    assert key and isinstance(key, str)
    assert isinstance(milliseconds, int) and milliseconds > 0

    return _encode_req('PEXPIRE', key, milliseconds)


def redis_hexists(key, field):
    """
    查看哈希表 key 中，给定域 field 是否存在
    """
    assert key and isinstance(key, str)
    assert field and isinstance(field, str)

    return _encode_req('HEXISTS', key, field)


def redis_setbit(key, offset, value):
    """
    设置二进制位
    """
    assert key and isinstance(key, str)
    assert isinstance(offset, int) and 0 <= offset <= 4294967296L
    assert isinstance(value, int) and value in (0, 1)

    return _encode_req('SETBIT', key, offset, value)


def redis_bitcount(key):
    """
    该函数没有限定范围, O(N)
    """
    assert key and isinstance(key, str)

    return _encode_req('BITCOUNT', key)


def redis_getbit(key, offset):
    assert key and isinstance(key, str)
    assert isinstance(offset, int) and 0 <= offset <= 4294967296L

    return _encode_req('GETBIT', key, offset)


def redis_setnx(key, value):
    """
    将 key 的值设为 value ，当且仅当 key 不存在
    """
    assert key and isinstance(key, str)
    assert value is not None

    return _encode_req('SETNX', key, value)


def redis_sismember(key, member):
    """
    判断 member 元素是否集合 key 的成员
    """
    assert key and isinstance(key, str)
    assert member and isinstance(member, str)

    return _encode_req('SISMEMBER', key, member)


def redis_sadd(key, member):
    """添加hashset成员
    """
    assert key and isinstance(key, str)
    assert isinstance(member, str)

    return _encode_req('SADD', key, member)


def redis_srem(key, member):
    """
    移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略
    """
    assert key and isinstance(key, str)
    assert member is not None

    return _encode_req('SREM', key, member)


def redis_scard(key):
    """Returns the set cardinality (number of elements) of the set stored at key
    """
    assert key and isinstance(key, str)

    return _encode_req('SCARD', key)


def redis_incre(key):
    assert key and isinstance(key, str)

    return _encode_req('INCR', key)


def redis_hincrby(key, field, increment):
    assert key and isinstance(key, str)
    assert field and isinstance(field, str)
    assert isinstance(increment, int)
    return _encode_req('HINCRBY', key, field, increment)


def redis_incrby(key, value):
    assert key and isinstance(key, str)
    assert isinstance(value, int)

    return _encode_req('INCRBY', key, value)


def redis_incre_byfloat(key, f):
    assert key and isinstance(key, str)
    assert isinstance(f, float)

    return _encode_req('INCRBYFLOAT', key, f)


def redis_smembers(key):
    assert key and isinstance(key, str)

    return _encode_req('SMEMBERS', key)


def redis_lpop(key):
    """从列表中弹出首元素

    :param key: 键值
    """
    assert key and isinstance(key, str)
    return _encode_req('LPOP', key)


def redis_lindex(key, index):
    assert key and isinstance(key, str)
    assert isinstance(index, int)
    return _encode_req('LINDEX', index)
    

def redis_blpop(timeout, *keys):
    """从列表中弹出首元素
    根据redis 2.8(其他版本未确认)实现方式，blpop指令有如下特征：
    * 对于未开启事务的pipeline
        1. 如果list存在至少一个元素，则返回首元素，
        2. 如果list为空，则blpop指令无任何意义，且包含blpop在内及其以后的指令均无回复
    * 在事务中，表现行为和lpop一致，不阻塞
    
    具体到项目中，要达到阻塞的目的，要注意以下几点：
    * 必须关闭事务选项
    * 禁止pipeline
    * 禁止在server中使用，只能在mq中使用
    * 禁止和AUTH, SELECT等一起使用形成pipeline
        
    :param timeout: 超时时间，一般为0，表示一直阻塞
    :param keys: 键值列表
    """
    assert isinstance(timeout, int) and timeout >= 0
    assert len(keys) >= 1
    
    params = list(tuple(keys))
    params.append(timeout)
    return _encode_req('BLPOP', *params)


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

    return _encode_req('RPUSH', key, value)


def chain_select_cmd(auth_pwd, select_db):
    """
    选择库的同时发送指令，作为一个pipe
    """
    if auth_pwd:
        if not isinstance(auth_pwd, str):
            raise ValueError('auth_pwd invalid: {0}'.format(auth_pwd))
    if not (isinstance(select_db, int) and 0 <= select_db <= 15):
        raise ValueError('select_db invalid: {0}'.format(select_db))

    return ''.join((
        '' if not auth_pwd else _encode_req('AUTH', auth_pwd),
        _encode_req('SELECT', select_db)
    ))