import json
from functools import partial
import requests
from requests import Session

debugger = 0
MAX_TRIES = 5
# TODO generate urls for better communication/documentation.

class Redis(object):
    '''
    benefits of OO IF:
      no shadowing of builtin set.
    caveat:
      must not use across threads
    problem:
      self.del => SyntaxError
    >>> url = 'http://blah.blah.org:6473'
    >>> redis = Redis(url, Session())
    '''
    def __init__(self, url, session):
        # wait!   Do we want to pass in a session? explicitly?  Good idea?
        # No need as long as one instance of this class per thread.
        # or shall we switch back to non-session?
        cmd = generate_cmd_func(url)
        cmds = '''get set ttl expire keys type 
        ping
        incr decr
        scard smembers sadd
        zcard 
        llen lrange lindex lpush
        '''
        for redis_cmd in cmds.split(): 
            self.__setattr__(redis_cmd, partial(cmd, redis_cmd.upper()))
        self.delete = partial(cmd, 'DEL')
#        self.del = self.delete
        # why does redis.del => syntax error


def generate_request_func(url, session, timeout=10):
    '''Return a function that uses a given session and url for sending
    requests.
    >>> req = generate_request_func(url, session, timeout=10)
    '''
    def req(verb, uri, i=1, data=None, headers={}):
        '''Base request function.
        i is for recursive retries.
        >>> req(verb, uri, data=None, headers={})  # verb is usually GET
        >>> _ = req('GET',   '/DEL/foo').text
        >>> req('GET',   '/GET/foo').text
        u'{"GET":null}'
        >>> req('GET',   '/SET/foo/bar').text
        u'{"SET":[true,"OK"]}'
        >>> req('GET',   '/GET/foo').text
        u'{"GET":"bar"}'
        >>> resp = req('GET',   '/GET/foo', headers={'User-Agent': 'Miller'})
        >>> resp.request.headers
        CaseInsensitiveDict({ ...  'User-Agent': 'Miller'})
        >>> req('PUT',   '/SET/foo', data='xxxxxxxxx').text 
        u'{"SET":[true,"OK"]}'
        >>> req('GET',   '/GET/foo').text 
        u'{"GET":"xxxxxxxxx"}'
        '''
        if data: verb = 'PUT'
        try:
            return session.request(verb.upper(), url+uri, data=data, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout:  # retry
            print '    retrying due to timeout', verb, uri, i+1, data, headers
            if i < MAX_TRIES:
                return req(verb, uri, i+1, data, headers)
            return 'too many tries!!!!!!!!'
    return req


def generate_cmd_func(url):
    '''Return a function that uses a given url to send Redis/webdis requests.
    Makes use of webdis-specific behavior.
    >>> cmd = generate_cmd_func(url)
    '''
    req =  generate_request_func(url, Session())
    def cmd(CMD, *args, **kw):
        '''Send requests to webdis url.
        >>> cmd = generate_cmd_func(url)
        >>> def get(key):          return cmd('GET', key)
        >>> def setit(key, value): return cmd('SET', key, value)
        >>> ttl    = partial(cmd, 'TTL')
        >>> expire = partial(cmd, 'EXPIRE')
        >>> _ = setit('foo', 'bar')
        >>> assert get('foo') == 'bar'
        >>> ttl('foo')
        -1
        >>> expire('foo', 99)
        1
        '''
        if 'headers' in kw: headers = kw.pop('headers')
        else:               headers = {}
        if 'data' in kw: data = kw.pop('data')
        else:            data = {}
        assert kw == {}
            
        verb = 'GET'
        if any('/' in str(a) for a in args):
            args = tuple(a.replace('/', '%2F') for a in args)
        uri = ''.join('/%s'%a for a in (CMD,)+args)
        response = req(verb, uri, headers=headers, data=data)
        if debugger:
            globals().update(locals())
        try: assert response.ok
        except AssertionError:
            globals().update(locals())
            raise
        global jp
        jp = json.loads(response.text)
        # raise exception if ....
        if type(jp[CMD]) is list:
            if jp[CMD][0] is False:
                [tf, x] = jp[CMD] # assertion
                if tf is False:
                    raise RedisException(x)
        # Here is where things get ugly, accomodating webdis quirks.
        if CMD == 'TYPE':
            [tf, x] = jp[CMD] # assertion
            if tf is True:
                return x
            else:
                globals().update(locals())
                raise RedisException
        return jp[CMD]
    return cmd


class RedisException(Exception): pass
class BadRedisKey(RedisException): pass


############################################################################
################################# Testing ##################################
############################################################################

'''Testing Redis Commands
'''

def setup():
    url = 'blah.blah.com' # 
    webdisurl = "http://%s:%s" %(url,7379)
    redis =  Redis(webdisurl, Session())
    req = generate_request_func(webdisurl, Session())
    cmd = generate_cmd_func(webdisurl)
    def get(key):          return cmd('GET', key)
    def setit(key, value): return cmd('SET', key, value)
    ttl    = partial(cmd, 'TTL')
    expire = partial(cmd, 'EXPIRE')
    debugger = 99
    globals().update(locals())
    # NOTE brevity of partial compared to explicitly defining functions.
    # NOTE also diff between passing wrong number of args to ttl vs get.  
    # get => Python exception dt too many args.
    # ttl => NO exception but a Redis error dt too many args.
    # The partial version is nice because it does not get between the user
    # and Redis.  It returns the unadorned Redis error.
    # But the explicit function protects from bonehead errors.
    # at the expense of verbosity.
 

def testit():
    print 'basic methods', test_basics()
    print 'set methods',   test_set_methods()
    print 'list methods',  test_list_methods()
    print 'stuff',         test_stuff()
    print 'violations',    test_violations()
    print 'passthrough',   test_passthrough()
    print 'timeout...',    test_timeout_retry()


def test_passthrough():
    '''Test parameters that get passed from the object to the underlying
    request.
    '''
    setup()
    # headers
    blah = redis.get('foo', headers={'User-Agent': 'Miller'} )
    assert response.request.headers['User-Agent'] == 'Miller'

    # data
    # If data is passed in the body we omit a corresponding parameter that
    # would otherwise be required.
    redis.set('foo', 'moo')
    assert redis.get('foo') == 'moo'
    redis.set('foo', data='goo')
    assert redis.get('foo') == 'goo'
    try:
        redis.set('foo', 'moo', data='goo')
    except RedisException: # too many parameters => redis syntax error
        pass
    return 'ok'


def test_timeout_retry(n=2, sleeptime=180):
    '''Reproduce intermittent connection error from requests.
    When running interactively it always works to Ctl-C + retry.
    But running non-interactively => crash.
    Sleeptime is the important factor; 3 minute sleeptime reliably triggers
    the retry.
    '''
    import time
    print 'sleeping for', sleeptime
    time.sleep(sleeptime)

    try: redis
    except NameError: setup()
    _ = redis.delete('foo')
    for i in range(10**n):
        inc = redis.incr('foo')
        x = redis.get('foo')
        if i%10 == 0:   # every 10th
            v = redis.get('foo')
        if i%100 == 0:   # every 100th
            w = redis.get('foo')
        msg = '%s/%s %s %s %s %s' %(i, 10**n, inc, v, w, inc%111)
        print_in_place(msg)
    print
    return 'timeout/retry test ok'    

 
def test_violations():
    setup()
    assert redis.delete('foo') in (0,1)
    try:
        redis.get('foo', 'bar')
    except RedisException, exc:
        assert exc.args[0] == "ERR wrong number of arguments for 'get' command"
    assert redis.lpush('foo', 5) == 1
    try:
        redis.get('foo')
    except RedisException, exc:
        assert exc.args[0] == 'WRONGTYPE Operation against a key holding the wrong kind of value'
    assert redis.delete('foo') == 1
    return 'ok'
 
 
def test_basics():
    '''get/set/ttl/expire/delete
    '''
    setup()
    assert redis.delete('foo') in (0,1)
    assert redis.get('foo') is None
    assert redis.ttl('foo') == -2
    assert redis.set('foo', 1) == [True, 'OK']
    assert redis.ttl('foo') == -1
    assert redis.expire('foo', 111) == 1
    assert redis.ttl('foo') == 111
    assert redis.delete('foo') == 1
    return 'ok'

    
def test_stuff():
    '''keys ping incr decr
    '''
    setup()
    assert redis.delete('foo') in (0,1)
    assert len(redis.keys('seq*'))
    assert redis.ping() == [True, 'PONG']
    for i in range(1, 11): assert redis.incr('foo') == i
    for i in range(7): assert redis.decr('foo') == 9-i
    assert int(redis.get('foo')) == 3
    assert redis.delete('foo') == 1
    return 'ok'


def test_list_methods():
    setup()
    assert redis.delete('foo') in (0,1)
    for (i, ob) in enumerate(['x', 'y', 1]):
        assert redis.lpush('foo', ob) == i+1
    assert redis.llen('foo') == 3
    assert redis.lrange('foo', 2, 3) == ['x']
    assert redis.lindex('foo', 2) == 'x'
    assert redis.delete('foo') == 1
    return 'ok'


def test_set_methods():
    setup()
    assert redis.delete('foo') in (0,1)
    for s in 'abc def ghi'.split():
        assert redis.sadd('foo', s) == 1
    assert redis.scard('foo') == 3
    assert sorted(redis.smembers('foo')) == sorted('abc def ghi'.split())
    assert redis.delete('foo') == 1
    return 'ok'
      

def first_mismatch(s1, s2):
    '''index of first mismatch in two strings.
    >>> first_mismatch('abcd', 'abcd')
    >>> first_mismatch('abcd', 'abxy')
    (2, 'c', 'x')
    >>> first_mismatch('abcd', 'abc')
    (3, 'd', '')
    >>> first_mismatch('abcd', 'abcde')
    (4, '', 'e')
    '''
    for (i,(a,b)) in enumerate(zip(s1, s2)):
        if a!=b:
            return (i,a,b)
    L1 = len(s1)
    L2 = len(s2)
    if L1 < L2:
        return (i+1, '', s2[i+1])
    if L1 > L2:
        return (i+1, s1[i+1], '')


def print_in_place(msg):
    sys.stdout.write(u"\r\x1b[K"+unicode(msg))
    sys.stdout.flush()

