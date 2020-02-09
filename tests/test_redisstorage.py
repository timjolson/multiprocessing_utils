import pytest
from mp_utils import MPStorage
import sys
import logging
# logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

address = ('', 23456)
authkey = b'local:23456'


def test_constructor():
    server = MPStorage(address, authkey)
    client = MPStorage(address, authkey)
    server.start()
    client.start()

    assert server.access == 'server'
    assert client.access == 'client'

    server2 = MPStorage(('', 23457), authkey)
    assert server2.access is None
    server2.start()
    assert server2.access == 'server'

    del client, server, server2


def test_locks():
    server = MPStorage(address, authkey).start()
    client = MPStorage(address, authkey).start()

    serverlock = server.getLock('test_with')
    clientlock = client.getLock('test_with')

    with serverlock:
        assert clientlock.acquire(False) is False
    with clientlock:
        assert serverlock.acquire(False) is False
    with server.getLock('test_with'):
        assert client.getLock('test_with').acquire(False) is False
    with client.getLock('test_with'):
        assert server.getLock('test_with').acquire(False) is False

    clientlock = client.getLock('test_acquire')
    serverlock = server.getLock('test_acquire')

    assert clientlock.acquire() is True
    assert serverlock.acquire(False) is False
    clientlock.release()
    assert serverlock.acquire() is True
    assert clientlock.acquire(False) is False
    serverlock.release()
    assert client.getLock('test_acquire').acquire() is True
    assert server.getLock('test_acquire').acquire(False) is False
    client.getLock('test_acquire').release()
    assert server.getLock('test_acquire').acquire() is True
    assert client.getLock('test_acquire').acquire(False) is False
    server.getLock('test_acquire').release()

    del client, server


def test_list():
    server = MPStorage(address, authkey).start()
    client = MPStorage(address, authkey).start()

    serverList = server.getList('test_list_append')
    clientList = client.getList('test_list_append')
    assert list(serverList) == list(clientList)

    clientList.append('client')
    serverList.append('server')

    assert list(clientList) == list(serverList) == ['client', 'server']

    serverList = server.getList('test_modify')
    clientList = client.getList('test_modify')
    assert list(serverList) == list(clientList)

    clientList.append('client')
    serverList.append('server')
    clientList[0] = 'client_modified'
    serverList[1] = 'server_modified'

    assert list(clientList) == list(serverList) == ['client_modified', 'server_modified']

    del client, server


def test_q():
    server = MPStorage(address, authkey).start()
    client = MPStorage(address, authkey).start()

    serverq = server.getQueue('Q')
    clientq = client.getQueue('Q')

    serverq.put('from server')
    serverq.put('put')
    assert clientq.get() == 'from server'
    assert serverq.get() == 'put'

    clientq.put('from client')
    clientq.put('put')
    assert serverq.get() == 'from client'
    assert clientq.get() == 'put'

    del client, server

