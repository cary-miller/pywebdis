import sys

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


def test_print_in_place(n):
    import time
    for i in range(n):
        print_in_place('%s/%s' %(i,n))
        time.sleep(.001)
    print

