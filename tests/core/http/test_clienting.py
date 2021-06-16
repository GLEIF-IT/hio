# -*- coding: utf-8 -*-
"""
Unittests for http clienting module
"""
import sys
import os
import time
from urllib.parse import urlsplit, quote_plus, unquote



import pytest

from hio import help
from hio.help import helping
from hio.help.helping import imdict
from hio.base import tyming
from hio.core import wiring, tcp
from hio.core.http import clienting


logger = help.ogler.getLogger()

tlsdirpath = os.path.dirname(
                os.path.dirname(
                        os.path.abspath(
                            sys.modules.get(__name__).__file__)))
certdirpath = os.path.join(tlsdirpath, 'tls', 'certs')


def mockEchoService(server):
    """
    mock echo server service for testing
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
               b'Host: 127.0.0.1:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Accept: application/json\r\n\r\n')

    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if  msgIn == mockMsg:
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 200 OK\r\n'
                      b'Content-Length: 122\r\n'
                      b'Content-Type: application/json\r\n'
                      b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
                      b'Server: IoBook.local\r\n\r\n'
                      b'{"content": null, '
                      b'"query": {"name": "fame"}, '
                      b'"verb": "GET", '
                      b'"url": "http://127.0.0.1:8080/echo?name=fame", '
                      b'"action": null}')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()


def mockEchoServiceLocalhost(server):
    """
    mock echo server service utility function
    localhost instead of 127.0.0.1
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
                b'Host: localhost:6101\r\n'
                b'Accept-Encoding: identity\r\n'
                b'Accept: application/json\r\n\r\n')
    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if  msgIn == mockMsg:
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 200 OK\r\n'
                      b'Content-Length: 122\r\n'
                      b'Content-Type: application/json\r\n'
                      b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
                      b'Server: IoBook.local\r\n\r\n'
                      b'{"content": null, '
                      b'"query": {"name": "fame"}, '
                      b'"verb": "GET", '
                      b'"url": "http://127.0.0.1:8080/echo?name=fame", '
                      b'"action": null}')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()


def mockEchoServiceJson(server):
    """
    mock echo server service with json data request body utility function
    """
    mockMsg = (b'PUT /echo?name=fame HTTP/1.1\r\n'
               b'Host: localhost:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Content-Length: 31\r\n'
               b'Accept: application/json\r\n'
               b'Content-Type: application/json; charset=utf-8\r\n\r\n'
               b'{"first":"John","last":"Smith"}')

    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn == mockMsg:
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 200 OK\r\n'
                      b'Content-Length: 122\r\n'
                      b'Content-Type: application/json\r\n'
                      b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
                      b'Server: IoBook.local\r\n\r\n'
                      b'{"content": null, '
                      b'"query": {"name": "fame"}, '
                      b'"verb": "GET", '
                      b'"url": "http://127.0.0.1:8080/echo?name=fame", '
                      b'"action": null}')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()


def mockRedirectService(server):
    """
    mock echo server service utility function
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
               b'Host: 127.0.0.1:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Accept: application/json\r\n\r\n')
    mockRedirectMsg = (b'GET /redirect?name=fame HTTP/1.1\r\n'
                       b'Host: 127.0.0.1:6101\r\n'
                       b'Accept-Encoding: identity\r\n'
                       b'Accept: application/json\r\n\r\n')
    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn == mockMsg:
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 307 Temporary Redirect\r\n'
                      b'Content-Type: text/plain\r\n'
                      b'Content-Length: 0\r\n'
                      b'Access-Control-Allow-Origin: *\r\n'
                      b'Location: http://localhost:6101/redirect?name=fame\r\n\r\n')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        elif  msgIn== mockRedirectMsg:
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 200 OK\r\n'
                      b'Content-Length: 122\r\n'
                      b'Content-Type: application/json\r\n'
                      b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
                      b'Server: IoBook.local\r\n\r\n'
                      b'{"content": null, '
                      b'"query": {"name": "fame"}, '
                      b'"verb": "GET", '
                      b'"url": "http://127.0.0.1:8080/echo?name=fame", '
                      b'"action": null}')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()


def mockRedirectComplexServiceA(server):
    """
    mock echo server service utility function
    """


    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = server.ixes.values()[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn == b'GET /echo?name=fame HTTP/1.1\r\nHost: 127.0.0.1:6101\r\nAccept-Encoding: identity\r\nAccept: application/json\r\n\r\n':
            ixClient.clearRxbs()
            msgOut = b'HTTP/1.1 307 Temporary Redirect\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nAccess-Control-Allow-Origin: *\r\nLocation: http://localhost:6103/redirect?name=fame\r\n\r\n'
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()

def mockRedirectComplexServiceG(server):
    """
    mock echo server service utility function
    """
    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = server.ixes.values()[0]
        msgIn = bytes(ixClient.rxbs)

        if  msgIn== b'GET /redirect?name=fame HTTP/1.1\r\nHost: localhost:6103\r\nAccept-Encoding: identity\r\nAccept: application/json\r\n\r\n':
            ixClient.clearRxbs()
            msgOut = b'HTTP/1.1 200 OK\r\nContent-Length: 122\r\nContent-Type: application/json\r\nDate: Thu, 30 Apr 2015 19:37:17 GMT\r\nServer: IoBook.local\r\n\r\n{"content": null, "query": {"name": "fame"}, "verb": "GET", "url": "http://127.0.0.1:8080/echo?name=fame", "action": null}'
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()



def test_client_request_echo():
    """
    Test HTTP Client request echo non blocking
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    method = u'GET'
    path = u'/echo?name=fame'
    headers = dict([('Accept', 'application/json')])

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # connect Client Beta to Server Alpha
    while True:
        beta.connector.serviceConnect()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.cs.getpeername() == beta.connector.cs.getsockname()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha, beta.connector.ha

    #  build request
    msgOut = beta.requester.rebuild()
    assert  beta.requester.lines == [
               b'GET /echo?name=fame HTTP/1.1',
               b'Host: 127.0.0.1:6101',
               b'Accept-Encoding: identity',
               b'Accept: application/json',
               b'',
               b'',
            ]

    assert beta.requester.head == (b'GET /echo?name=fame HTTP/1.1\r\n'
                                   b'Host: 127.0.0.1:6101\r\nAccept-Encoding: '
                                   b'identity\r\nAccept: application/json\r\n\r\n')
    assert msgOut == (b'GET /echo?name=fame HTTP/1.1\r\nHost: 127.0.0.1:6101\r\n'
                      b'Accept-Encoding: identity\r\nAccept: application/json\r\n\r\n')


    beta.connector.tx(msgOut)
    while beta.connector.txbs and not ixBeta.rxbs :
        beta.connector.serviceSends()
        time.sleep(0.05)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
    msgIn = bytes(ixBeta.rxbs)
    assert msgIn == msgOut
    ixBeta.clearRxbs()

    # build resonse
    msgOut = (b'HTTP/1.1 200 OK\r\n'
              b'Content-Length: 122\r\n'
              b'Content-Type: application/json\r\n'
              b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
              b'Server: IoBook.local\r\n\r\n'
              b'{"content": null, "query": {"name": "fame"}, "verb": "GET", '
              b'"url": "http://127.0.0.1:8080/echo?name=fame", "action": null}')

    ixBeta.tx(msgOut)
    while ixBeta.txbs or not beta.connector.rxbs:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        beta.connector.serviceReceives()
        time.sleep(0.05)
    msgIn = bytes(beta.connector.rxbs)
    assert msgIn == msgOut

    while beta.respondent.parser:
        beta.respondent.parse()

    assert not beta.connector.rxbs
    assert list(beta.respondent.headers.items()) ==  [('Content-Length', '122'),
                                                      ('Content-Type', 'application/json'),
                                                      ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'),
                                                      ('Server', 'IoBook.local')]

    beta.respondent.dictify()  # convert JSON data in body

    assert beta.respondent.body == (b'{"content": null, '
                                           b'"query": {"name": "fame"}, '
                                           b'"verb": "GET", "url'
                                           b'": "http://127.0.0.1:8080/echo?name=fame", '
                                           b'"action": null}')
    assert beta.respondent.data == {'action': None,
                                     'content': None,
                                     'query': {'name': 'fame'},
                                     'url': 'http://127.0.0.1:8080/echo?name=fame',
                                     'verb': 'GET'}


    alpha.close()
    beta.connector.close()


def test_client_service_all_echo():
    """
    Test Client serviceAll echo nonblocking
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    method = u'GET'
    path = u'/echo?name=fame'
    headers = dict([('Accept', 'application/json')])

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # connect Client Beta to Server Alpha
    while True:
        beta.connector.serviceConnect()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.cs.getpeername() == beta.connector.cs.getsockname()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha, beta.connector.ha


    # build request as result of transmit
    beta.transmit()
    assert beta.requester.lines == [
               b'GET /echo?name=fame HTTP/1.1',
               b'Host: 127.0.0.1:6101',
               b'Accept-Encoding: identity',
               b'Accept: application/json',
               b'',
               b'',
            ]


    assert beta.requester.head == (b'GET /echo?name=fame HTTP/1.1\r\n'
                                   b'Host: 127.0.0.1:6101\r\n'
                                   b'Accept-Encoding: identity\r\n'
                                   b'Accept: application/json\r\n\r\n')
    msgOut = bytes(beta.connector.txbs)  # make copy
    assert msgOut == (b'GET /echo?name=fame HTTP/1.1\r\n'
                      b'Host: 127.0.0.1:6101\r\n'
                      b'Accept-Encoding: identity\r\n'
                      b'Accept: application/json\r\n\r\n')

    while beta.connector.txbs and not ixBeta.rxbs :
        beta.serviceAll()
        time.sleep(0.05)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
    msgIn = bytes(ixBeta.rxbs)
    assert msgIn == msgOut
    ixBeta.clearRxbs()

    # build response
    msgOut = (b'HTTP/1.1 200 OK\r\n'
              b'Content-Length: 122\r\n'
              b'Content-Type: application/json\r\n'
              b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
              b'Server: IoBook.local\r\n\r\n'
              b'{"content": null, '
              b'"query": {"name": "fame"}, '
              b'"verb": "GET", '
              b'"url": "http://127.0.0.1:8080/echo?name=fame", '
              b'"action": null}')
    ixBeta.tx(msgOut)
    while ixBeta.txbs or not beta.respondent.ended:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended
    assert len(beta.responses) == 1

    assert list(beta.respondent.headers.items()) == [('Content-Length', '122'),
                                                     ('Content-Type', 'application/json'),
                                                     ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'),
                                                     ('Server', 'IoBook.local')]

    assert beta.respondent.body == (b'{"content": null, '
                                    b'"query": {"name": "fame"}, '
                                    b'"verb": "GET", "url'
                                    b'": "http://127.0.0.1:8080/echo?name=fame", '
                                    b'"action": null}')
    assert beta.respondent.data == {'action': None,
                                     'content': None,
                                     'query': {'name': 'fame'},
                                     'url': 'http://127.0.0.1:8080/echo?name=fame',
                                     'verb': 'GET'}

    alpha.close()
    beta.connector.close()


def test_client_pipeline_echo():
    """
    Test Client pipeline servicing
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    method = u'GET'
    path = u'/echo?name=fame'
    headers = dict([('Accept', 'application/json')])

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # connect Client Beta to Server Alpha
    while True:
        beta.connector.serviceConnect()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.cs.getpeername() == beta.connector.cs.getsockname()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha, beta.connector.ha

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (beta.requests or beta.connector.txbs) and not ixBeta.rxbs :
        beta.serviceAll()
        time.sleep(0.05)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
    msgIn = bytes(ixBeta.rxbs)

    assert msgIn == (b'GET /echo?name=fame HTTP/1.1\r\n'
                     b'Host: 127.0.0.1:6101\r\n'
                     b'Accept-Encoding: identity\r\n'
                     b'Accept: application/json\r\n\r\n')


    ixBeta.clearRxbs()  # ensure no stale stuff in beta rx buffer at Alpha

    #  build response
    msgOut = (b'HTTP/1.1 200 OK\r\n'
              b'Content-Length: 122\r\n'
              b'Content-Type: application/json\r\n'
              b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
              b'Server: IoBook.local\r\n\r\n'
              b'{"content": null, '
              b'"query": {"name": "fame"}, '
              b'"verb": "GET", '
              b'"url": "http://127.0.0.1:8080/echo?name=fame", '
              b'"action": null}')
    ixBeta.tx(msgOut)
    while ixBeta.txbs or not beta.respondent.ended:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': {'Content-Length': '122',
                                           'Content-Type': 'application/json',
                                           'Date': 'Thu, 30 Apr 2015 19:37:17 GMT',
                                           'Server': 'IoBook.local'},
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': {'Accept': 'application/json'},
                                    'body': b'',
                                    'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}


    # resend request in pipeline mode
    beta.requests.append(request)
    while ( beta.requests or beta.connector.txbs) and not ixBeta.rxbs :
        beta.serviceAll()
        time.sleep(0.05)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
    msgIn = bytes(ixBeta.rxbs)
    assert msgIn == (b'GET /echo?name=fame HTTP/1.1\r\n'
                     b'Host: 127.0.0.1:6101\r\n'
                     b'Accept-Encoding: identity\r\n'
                     b'Accept: application/json\r\n\r\n')

    ixBeta.clearRxbs()

    # build response
    msgOut =( b'HTTP/1.1 200 OK\r\n'
              b'Content-Length: 122\r\n'
              b'Content-Type: application/json\r\n'
              b'Date: Thu, 30 Apr 2015 19:37:17 GMT\r\n'
              b'Server: IoBook.local\r\n\r\n'
              b'{"content": null, '
              b'"query": {"name": "fame"}, '
              b'"verb": "GET", '
              b'"url": "http://127.0.0.1:8080/echo?name=fame", '
              b'"action": null}')
    ixBeta.tx(msgOut)
    while ixBeta.txbs or not beta.respondent.ended:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                                'status': 200,
                                'reason': 'OK',
                                'headers':
                                    {'Content-Length': '122',
                                     'Content-Type': 'application/json',
                                     'Date': 'Thu, 30 Apr 2015 19:37:17 GMT',
                                     'Server': 'IoBook.local'},
                                'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                                b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                                b'on": null}'),
                                'data': {'action': None,
                                         'content': None,
                                         'query': {'name': 'fame'},
                                         'url': 'http://127.0.0.1:8080/echo?name=fame',
                                         'verb': 'GET'},
                                'error': None,
                                'errored': False,
                                'request': {'host': '127.0.0.1',
                                            'port': 6101,
                                            'scheme': 'http',
                                            'method': 'GET',
                                            'path': '/echo',
                                            'qargs': {'name': 'fame'},
                                            'fragment': '',
                                            'headers': {'Accept': 'application/json'},
                                            'body': b'',
                                            'data': None,
                                            'fargs': None,
                                            }
                                }

    alpha.close()
    beta.connector.close()




def test_client_pipeline_echo_simple():
    """
    Test CLient pipeline servicing
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    method = u'GET'
    path = u'/echo?name=fame'
    headers = dict([('Accept', 'application/json')])

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # connect Client Beta to Server Alpha
    while True:
        beta.connector.serviceConnect()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.cs.getpeername() == beta.connector.cs.getsockname()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha, beta.connector.ha

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or  beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.respond()  # returns next response from .responses
    assert response == clienting.Response(version=(1, 1),
                                          status=200,
                                          reason='OK',
                                          headers=imdict([('Content-Length', '122'),
                                                          ('Content-Type', 'application/json'),
                                                          ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'),
                                                          ('Server', 'IoBook.local')]),
                                          body=bytearray(b'{"content": null, '
                                                         b'"query": {"name": "fame"}, '
                                                         b'"verb": "GET", '
                                                         b'"url": "http://127.0.0.1:8080/echo?name=fame", '
                                                         b'"action": null}'),
                                          data={'content': None,
                                                'query': {'name': 'fame'},
                                                'verb': 'GET',
                                                'url': 'http://127.0.0.1:8080/echo?name=fame',
                                                'action': None},
                                          request={'method': 'GET',
                                                   'path': '/echo',
                                                   'qargs': {'name': 'fame'},
                                                   'fragment': '',
                                                   'headers': imdict([('Accept', 'application/json')]),
                                                   'body': b'',
                                                   'host': '127.0.0.1',
                                                   'port': 6101,
                                                   'scheme': 'http',
                                                   'data': None,
                                                   'fargs': None},
                                          errored=False,
                                          error=None)


    #  pipeline request to send again
    beta.requests.append(request)

    while (not alpha.ixes or  beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.connector.close()


def test_client_echo_simple_host_port_path():
    """
    Test Client Simple First time request pattern
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    method = u'GET'
    path = u'/echo?name=fame'
    headers = dict([('Accept', 'application/json')])

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    beta.transmit()

    while (not alpha.ixes or  beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'method': 'GET',
                                    'path': '/echo',
                                    'fragment': '',
                                    'qargs': {'name': 'fame'},
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    beta.request(method=u'GET',
                  path=u'/echo?name=fame',
                  headers=dict([('Accept', 'application/json')]))

    while (not alpha.ixes or  beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended


    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None,
                                    'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http'},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.connector.close()


def test_client_pipline_echo_simple_path_scheme():
    """
    Test Patron pipeline servicing using path components for host port scheme
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)


    path = "http://{0}:{1}/".format('localhost', alpha.eha[1])  # needed for connect
    # method and headers set in request
    beta = clienting.Patron(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    beta.requests.append(request)  # pipeline request

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}
    alpha.close()
    beta.connector.close()


def test_client_pipeline_echo_simple_path_track():
    """
    Test Patron pipeline servicing using path components for host port scheme
    Request includes tracking information that is included in reponses copy
    of request
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)


    path = "http://{0}:{1}/".format('localhost', alpha.eha[1])  # needed for connect
    # method and headers set in request
    beta = clienting.Patron(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                     ('mid', 1),  # tracking
                     ('drop', '.stuff.reply'),  # tracking
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'mid': 1,
                                    'drop': '.stuff.reply',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    request.update(mid=2, drop='.puff.reply')
    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'mid': 2,
                                    'drop': '.puff.reply',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.connector.close()


def test_client_pipeline_echo_json():
    """
    Test Client pipeline servicing using path components for host port scheme
    with json body in data
    Request includes tracking information that is included in reponses copy
    of request
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)


    path = "http://{0}:{1}/".format('localhost', alpha.eha[1])  # needed for connect
    # method and headers set in request
    beta = clienting.Patron(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    request = dict([('method', u'PUT'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('data', dict([("first", "John"), ("last", "Smith")])),
                     ('mid', 1),
                     ('drop', '.stuff.reply'),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceJson(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'PUT',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json'), ('content-type', 'application/json; charset=utf-8')]),
                                    'data': {'first': 'John', 'last': 'Smith'},
                                    'mid': 1,
                                    'drop': '.stuff.reply',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'body': b'',
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    request.update(mid=2, drop='.puff.reply')
    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
            beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceJson(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'PUT',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json'), ('content-type', 'application/json; charset=utf-8')]),
                                    'data': {'first': 'John', 'last': 'Smith'},
                                    'mid': 2,
                                    'drop': '.puff.reply',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'body': b'',
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.connector.close()


def test_client_pipeline_sse_stream():
    """
    Test Client pipeline stream sse
    """
    tymist = tyming.Tymist(tyme=0.0)

    alpha = tcp.Server(port = 6101,
                       bufsize=131072,
                       tymth=tymist.tymen())
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    beta = clienting.Patron(bufsize=131072,
                             hostname=host,
                             port=port,
                             tymth=tymist.tymen(),  # passed through to connector
                             reconnectable=True,  # passed through to connector
                             tymeout=1.0,  # passed through to connector
                             )
    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    while True:
        beta.serviceAll()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.cs.getpeername() == beta.connector.cs.getsockname()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha == beta.connector.ha

    # build request
    request = dict([('method', u'GET'),
                     ('path', u'/stream'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)
    while (beta.requests or beta.connector.txbs) and not ixBeta.rxbs:
        beta.serviceAll()
        time.sleep(0.05)
        tymist.tick(tock=0.5)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    msgIn = bytes(ixBeta.rxbs)
    assert msgIn == (b'GET /stream HTTP/1.1\r\n'
                     b'Host: 127.0.0.1:6101\r\n'
                     b'Accept-Encoding: identity\r\n'
                     b'Accept: application/json\r\n\r\n')

    ixBeta.clearRxbs()

    # build response
    lines = [
        b'HTTP/1.0 200 OK\r\n',
        b'Server: IoBook.local\r\n',
        b'Date: Thu, 30 Apr 2015 21:35:25 GMT\r\n'
        b'Content-Type: text/event-stream\r\n',
        b'Cache-Control: no-cache\r\n',
        b'Connection: close\r\n\r\n',
        b'retry: 1000\n\n',
        b'id: 0\ndata: START\n\n',
        b'id: 1\ndata: 1\ndata: 2\n\n',
        b'id: 2\ndata: 3\ndata: 4\n\n',
        b'id: 3\ndata: 5\ndata: 6\n\n',
        b'id: 4\ndata: 7\ndata: 8\n\n',
    ]
    msgOut = b''.join(lines)
    ixBeta.tx(msgOut)
    tymer = tyming.Tymer(tymth=tymist.tymen(), duration=0.5)
    while ixBeta.txbs or not tymer.expired:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        tymist.tick(tock=0.5)
        beta.serviceAll()
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    # tymer expired  while stream still open so no responses in .responses
    assert not beta.connector.rxbs
    assert beta.waited
    assert not beta.respondent.ended
    assert len(beta.responses) == 0

    # but events made it into in .events
    assert len(beta.events) == 5
    assert beta.respondent.retry == 1000
    assert beta.respondent.leid == '4'
    event = beta.events.popleft()
    assert event == {'id': '0', 'name': '', 'data': 'START'}
    event = beta.events.popleft()
    assert event == {'id': '1', 'name': '', 'data': '1\n2'}
    event = beta.events.popleft()
    assert event == {'id': '2', 'name': '', 'data': '3\n4'}
    event = beta.events.popleft()
    assert event == {'id': '3', 'name': '', 'data': '5\n6'}
    event = beta.events.popleft()
    assert event == {'id': '4', 'name': '', 'data': '7\n8'}
    assert not beta.events

    # shutdown alpha's ixBeta connection prematurely so beta must reconnect
    alpha.closeIx(beta.connector.ca)
    alpha.removeIx(beta.connector.ca)
    while True:
        beta.serviceAll()
        if not beta.connector.connected:
            break
        time.sleep(0.1)
        tymist.tick(tock=0.5)

    assert not beta.connector.cutoff

    # auto reconnect
    while True:
        beta.serviceAll()
        alpha.serviceConnects()
        if beta.connector.connected and beta.connector.ca in alpha.ixes:
            break
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    assert beta.connector.accepted
    assert beta.connector.connected
    assert not beta.connector.cutoff
    assert beta.connector.ca == beta.connector.cs.getsockname()
    assert beta.connector.ha == beta.connector.cs.getpeername()
    assert alpha.eha == beta.connector.ha

    ixBeta = alpha.ixes[beta.connector.ca]
    assert ixBeta.ca is not None
    assert ixBeta.cs is not None
    assert ixBeta.cs.getsockname() == beta.connector.cs.getpeername()
    assert ixBeta.ca == beta.connector.ca
    assert ixBeta.ha == beta.connector.ha

    while (beta.requests or beta.connector.txbs) or not ixBeta.rxbs:
        beta.serviceAll()
        time.sleep(0.05)
        tymist.tick(tock=0.5)
        alpha.serviceReceivesAllIx()
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    msgIn = bytes(ixBeta.rxbs)
    assert msgIn == (b'GET /stream HTTP/1.1\r\n'
                     b'Host: 127.0.0.1:6101\r\n'
                     b'Accept-Encoding: identity\r\n'
                     b'Accept: application/json\r\n'
                     b'Last-Event-Id: 4\r\n\r\n')
    ixBeta.clearRxbs()

    # resume response
    lines = [
        b'HTTP/1.0 200 OK\r\n',
        b'Server: IoBook.local\r\n',
        b'Date: Thu, 30 Apr 2015 21:35:25 GMT\r\n'
        b'Content-Type: text/event-stream\r\n',
        b'Cache-Control: no-cache\r\n',
        b'Connection: close\r\n\r\n',
        b'id: 5\ndata: 9\ndata: 10\n\n',
        b'id: 6\ndata: 11\ndata: 12\n\n',
    ]

    msgOut = b''.join(lines)
    ixBeta.tx(msgOut)
    tymer = tyming.Tymer(tymth=tymist.tymen(), duration=0.5)
    while ixBeta.txbs or not tymer.expired:
        alpha.serviceSendsAllIx()
        time.sleep(0.05)
        tymist.tick(tock=0.5)
        beta.serviceAll()
        time.sleep(0.05)
        tymist.tick(tock=0.5)

    assert not beta.connector.rxbs

    #timed out while stream still open so no responses in .responses
    assert beta.waited
    assert not beta.respondent.ended
    assert len(beta.responses) == 0

    # but are events in .events
    assert len(beta.events) == 2
    assert beta.respondent.retry == 1000
    assert beta.respondent.leid == '6'
    event = beta.events.popleft()
    assert event == {'id': '5', 'name': '', 'data': '9\n10'}
    event = beta.events.popleft()
    assert event == {'id': '6', 'name': '', 'data': '11\n12'}

    alpha.close()
    beta.connector.close()



def test_client_pipline_echo_simple_tls():
    """
    Test Client pipeline servicing with TlS
    """
    #serverKeypath = '/etc/pki/tls/certs/server_key.pem'  # local server private key
    #serverCertpath = '/etc/pki/tls/certs/server_cert.pem'  # local server public cert
    #clientCafilepath = '/etc/pki/tls/certs/client.pem' # remote client public cert

    #clientKeypath = '/etc/pki/tls/certs/client_key.pem'  # local client private key
    #clientCertpath = '/etc/pki/tls/certs/client_cert.pem'  # local client public cert
    #serverCafilepath = '/etc/pki/tls/certs/server.pem' # remote server public cert

    serverKeypath = certdirpath + '/server_key.pem'  # local server private key
    serverCertpath = certdirpath + '/server_cert.pem'  # local server public cert
    clientCafilepath = certdirpath + '/client.pem' # remote client public cert

    clientKeypath = certdirpath + '/client_key.pem'  # local client private key
    clientCertpath = certdirpath + '/client_cert.pem'  # local client public cert
    serverCafilepath = certdirpath + '/server.pem' # remote server public cert

    serverCertCommonName = 'localhost' # match hostname uses servers's cert commonname

    alpha = tcp.ServerTls(host=serverCertCommonName,
                                  port = 6101,
                                  bufsize=131072,
                                  context=None,
                                  version=None,
                                  certify=None,
                                  keypath=serverKeypath,
                                  certpath=serverCertpath,
                                  cafilepath=clientCafilepath,
                                  )
    assert alpha.reopen()
    assert alpha.ha == ('127.0.0.1', 6101)
    assert alpha.eha, ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]

    beta = clienting.Patron(hostname=serverCertCommonName,
                          port=alpha.eha[1],
                          bufsize=131072,
                          scheme='https',
                          reconnectable=True,
                          certedhost=serverCertCommonName,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,
                        )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # build request
    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'https',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    # pipeline another request
    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'https',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.connector.close()


def test_client_pipeline_echo_simple_path_tls():
    """
    Test Client pipeline servicing with path input and tls
    """
        #serverKeypath = '/etc/pki/tls/certs/server_key.pem'  # local server private key
    #serverCertpath = '/etc/pki/tls/certs/server_cert.pem'  # local server public cert
    #clientCafilepath = '/etc/pki/tls/certs/client.pem' # remote client public cert

    #clientKeypath = '/etc/pki/tls/certs/client_key.pem'  # local client private key
    #clientCertpath = '/etc/pki/tls/certs/client_cert.pem'  # local client public cert
    #serverCafilepath = '/etc/pki/tls/certs/server.pem' # remote server public cert

    serverKeypath = certdirpath + '/server_key.pem'  # local server private key
    serverCertpath = certdirpath + '/server_cert.pem'  # local server public cert
    clientCafilepath = certdirpath + '/client.pem' # remote client public cert

    clientKeypath = certdirpath + '/client_key.pem'  # local client private key
    clientCertpath = certdirpath + '/client_cert.pem'  # local client public cert
    serverCafilepath = certdirpath + '/server.pem' # remote server public cert

    serverCertCommonName = 'localhost' # match hostname uses servers's cert commonname

    alpha = tcp.ServerTls(host=serverCertCommonName,
                                  port = 6101,
                                  bufsize=131072,
                                  context=None,
                                  version=None,
                                  certify=None,
                                  keypath=serverKeypath,
                                  certpath=serverCertpath,
                                  cafilepath=clientCafilepath,
                                  )
    assert alpha.reopen()
    assert alpha.ha == ('127.0.0.1', 6101)
    assert alpha.eha, ('127.0.0.1', 6101)

    path = "https://{0}:{1}/".format(serverCertCommonName, alpha.eha[1])

    beta = clienting.Patron(path=path,
                          bufsize=131072,
                          reconnectable=True,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,
                        )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # build request
    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses)== 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'method': 'GET',
                                    'path': '/echo',
                                    'qargs': {'name': 'fame'},
                                    'fragment': '',
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'https',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}


    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoServiceLocalhost(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses)== 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                         'status': 200,
                         'reason': 'OK',
                         'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                         'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                           b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                           b'on": null}'),
                         'data': {'content': None,
                                  'query': {'name': 'fame'},
                                  'verb': 'GET',
                                  'url': 'http://127.0.0.1:8080/echo?name=fame',
                                  'action': None},
                         'request': {'method': 'GET',
                                     'path': '/echo',
                                     'qargs': {'name': 'fame'},
                                     'fragment': '',
                                     'headers': imdict([('Accept', 'application/json')]),
                                     'body': b'',
                                     'host': 'localhost',
                                     'port': 6101,
                                     'scheme': 'https',
                                     'data': None,
                                     'fargs': None},
                         'errored': False,
                         'error': None}

    alpha.close()
    beta.connector.close()


def test_client_redirect_simple():
    """
    Test Patron redirect
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]

    beta = clienting.Patron(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 reconnectable=True,
                                 )

    assert beta.connector.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    # build request
    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockRedirectService(alpha)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    # response after redirect
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': imdict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'method': 'GET',
                                    'path': '/redirect',
                                    'fragment': '',
                                    'qargs': {'name': 'fame'},
                                    'headers': imdict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None,
                        'redirects': [{'version': (1, 1),
                                       'status': 307,
                                       'reason': 'Temporary Redirect',
                                       'headers': imdict([('Content-Type', 'text/plain'), ('Content-Length', '0'), ('Access-Control-Allow-Origin', '*'), ('Location', 'http://localhost:6101/redirect?name=fame')]),
                                       'body': bytearray(b''),
                                       'data': None,
                                       'request': {'method': 'GET',
                                                   'path': '/echo',
                                                   'qargs': {'name': 'fame'},
                                                   'fragment': '',
                                                   'headers': imdict([('Accept', 'application/json')]),
                                                   'body': b'',
                                                   'host': '127.0.0.1',
                                                   'port': 6101,
                                                   'scheme': 'http',
                                                   'data': None,
                                                   'fargs': None},
                                       'errored': False,
                                       'error': None}]}

    alpha.close()
    beta.connector.close()



def testPatronRedirectComplex():
    """
    Test Patron redirect
    """
    console.terse("{0}\n".format(self.testPatronRedirectComplex.__doc__))



    wireLogAlpha = wiring.WireLog(buffify=True, same=True)
    result = wireLogAlpha.reopen()

    alpha = tcp.Server(port = 6101, bufsize=131072, wlog=wireLogAlpha)
    self.assertIs(alpha.reopen(), True)
    self.assertEqual(alpha.ha, ('0.0.0.0', 6101))
    self.assertEqual(alpha.eha, ('127.0.0.1', 6101))

    wireLogGamma = wiring.WireLog(buffify=True, same=True)
    result = wireLogGamma.reopen()

    gamma = tcp.Server(port = 6103, bufsize=131072, wlog=wireLogGamma)
    self.assertIs(gamma.reopen(), True)
    self.assertEqual(gamma.ha, ('0.0.0.0', 6103))
    self.assertEqual(gamma.eha, ('127.0.0.1', 6103))

    console.terse("{0}\n".format("Building Connector ...\n"))

    wireLogBeta = wiring.WireLog(buffify=True,  same=True)
    result = wireLogBeta.reopen()
    host = alpha.eha[0]
    port = alpha.eha[1]

    beta = clienting.Patron(bufsize=131072,
                                 wlog=wireLogBeta,
                                 hostname=host,
                                 port=port,
                                 reconnectable=True,
                                 )

    self.assertIs(beta.connector.reopen(), True)
    self.assertIs(beta.connector.accepted, False)
    self.assertIs(beta.connector.connected, False)
    self.assertIs(beta.connector.cutoff, False)

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        self.mockRedirectComplexServiceA(alpha)
        self.mockRedirectComplexServiceG(gamma)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    self.assertEqual(len(beta.connector.rxbs), 0)
    self.assertIs(beta.waited, False)
    self.assertIs(beta.respondent.ended, True)

    self.assertEqual(len(beta.responses), 1)
    response = beta.responses.popleft()
    self.assertEqual(response, {'version': (1, 1),
                                'status': 200,
                                'reason': 'OK',
                                'headers':
                                    {'content-length': '122',
                                    'content-type': 'application/json',
                                    'date': 'Thu, 30 Apr 2015 19:37:17 GMT',
                                    'server': 'IoBook.local'},
                                'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                                b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                                b'on": null}'),
                                'data': {'action': None,
                                         'content': None,
                                         'query': {'name': 'fame'},
                                         'url': 'http://127.0.0.1:8080/echo?name=fame',
                                         'verb': 'GET'},
                                'error': None,
                                'errored': False,
                                'redirects': [{'body': bytearray(b''),
                                               'data': None,
                                               'headers': {'access-control-allow-origin': '*',
                                                           'content-length': '0',
                                                           'content-type': 'text/plain',
                                                           'location': 'http://localhost:6103/redirect?name=fame'},
                                               'reason': 'Temporary Redirect',
                                               'error': None,
                                               'errored': False,
                                               'request': {'body': b'',
                                                           'data': None,
                                                           'fargs': None,
                                                           'fragment': '',
                                                           'headers': {'accept': 'application/json'},
                                                           'host': '127.0.0.1',
                                                           'method': 'GET',
                                                           'path': '/echo',
                                                           'port': 6101,
                                                           'qargs': {'name': 'fame'},
                                                           'scheme': 'http'},
                                               'status': 307,
                                               'version': (1, 1)}],
                                'request':
                                    {'host': 'localhost',
                                     'port': 6103,
                                     'scheme': 'http',
                                     'method': 'GET',
                                     'path': '/redirect',
                                     'qargs': {'name': 'fame'},
                                     'fragment': '',
                                     'headers':
                                         {'accept': 'application/json'},
                                     'body': b'',
                                     'data': None,
                                     'fargs': None,
                                    }
                                })

    alpha.close()
    gamma.close()
    beta.connector.close()

    wireLogAlpha.close()
    wireLogGamma.close()
    wireLogBeta.close()



def mockRedirectComplexServiceASecure(server):
    """
    mock echo server service utility function
    """
    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = server.ixes.values()[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn == b'GET /echo?name=fame HTTP/1.1\r\nHost: localhost:6101\r\nAccept-Encoding: identity\r\nAccept: application/json\r\n\r\n':
            ixClient.clearRxbs()
            msgOut = b'HTTP/1.1 307 Temporary Redirect\r\nContent-Type: text/plain\r\nContent-Length: 0\r\nAccess-Control-Allow-Origin: *\r\nLocation: https://localhost:6103/redirect?name=fame\r\n\r\n'
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()

def mockRedirectComplexServiceGSecure(server):
    """
    mock echo server service utility function
    """
    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = server.ixes.values()[0]
        msgIn = bytes(ixClient.rxbs)

        if  msgIn== b'GET /redirect?name=fame HTTP/1.1\r\nHost: localhost:6103\r\nAccept-Encoding: identity\r\nAccept: application/json\r\n\r\n':
            ixClient.clearRxbs()
            msgOut = b'HTTP/1.1 200 OK\r\nContent-Length: 122\r\nContent-Type: application/json\r\nDate: Thu, 30 Apr 2015 19:37:17 GMT\r\nServer: IoBook.local\r\n\r\n{"content": null, "query": {"name": "fame"}, "verb": "GET", "url": "http://127.0.0.1:8080/echo?name=fame", "action": null}'
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()

def testPatronRedirectComplexSecure():
    """
    Test Patron redirect
    """
    console.terse("{0}\n".format(self.testPatronRedirectComplexSecure.__doc__))



    serverCertCommonName = 'localhost' # match hostname uses servers's cert commonname
    #serverKeypath = '/etc/pki/tls/certs/server_key.pem'  # local server private key
    #serverCertpath = '/etc/pki/tls/certs/server_cert.pem'  # local server public cert
    #clientCafilepath = '/etc/pki/tls/certs/client.pem' # remote client public cert

    serverKeypath = self.certdirpath + '/server_key.pem'  # local server private key
    serverCertpath = self.certdirpath + '/server_cert.pem'  # local server public cert
    clientCafilepath = self.certdirpath + '/client.pem' # remote client public cert

    wireLogAlpha = wiring.WireLog(buffify=True, same=True)
    result = wireLogAlpha.reopen()

    alpha = tcp.ServerTls(host=serverCertCommonName,
                               port = 6101,
                               bufsize=131072,
                               wlog=wireLogAlpha,
                               context=None,
                               version=None,
                               certify=None,
                               keypath=serverKeypath,
                               certpath=serverCertpath,
                               cafilepath=clientCafilepath,)
    self.assertIs(alpha.reopen(), True)
    self.assertEqual(alpha.ha, ('127.0.0.1', 6101))
    self.assertEqual(alpha.eha, ('127.0.0.1', 6101))

    wireLogGamma = wiring.WireLog(buffify=True, same=True)
    result = wireLogGamma.reopen()

    gamma = tcp.ServerTls(host=serverCertCommonName,
                               port = 6103,
                               bufsize=131072,
                               wlog=wireLogGamma,
                               context=None,
                               version=None,
                               certify=None,
                               keypath=serverKeypath,
                               certpath=serverCertpath,
                               cafilepath=clientCafilepath)
    self.assertIs(gamma.reopen(), True)
    self.assertEqual(gamma.ha, ('127.0.0.1', 6103))
    self.assertEqual(gamma.eha, ('127.0.0.1', 6103))

    console.terse("{0}\n".format("Building Connector ...\n"))

    #clientKeypath = '/etc/pki/tls/certs/client_key.pem'  # local client private key
    #clientCertpath = '/etc/pki/tls/certs/client_cert.pem'  # local client public cert
    #serverCafilepath = '/etc/pki/tls/certs/server.pem' # remote server public cert

    clientKeypath = self.certdirpath + '/client_key.pem'  # local client private key
    clientCertpath = self.certdirpath + '/client_cert.pem'  # local client public cert
    serverCafilepath = self.certdirpath + '/server.pem' # remote server public cert

    wireLogBeta = wiring.WireLog(buffify=True,  same=True)
    result = wireLogBeta.reopen()
    host = serverCertCommonName
    port = alpha.eha[1]

    beta = clienting.Patron(bufsize=131072,
                          wlog=wireLogBeta,
                          hostname=host,
                          port=port,
                          reconnectable=True,
                          scheme='https',
                          certedhost=serverCertCommonName,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,)

    self.assertIs(beta.connector.reopen(), True)
    self.assertIs(beta.connector.accepted, False)
    self.assertIs(beta.connector.connected, False)
    self.assertIs(beta.connector.cutoff, False)

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (not alpha.ixes or beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        self.mockRedirectComplexServiceASecure(alpha)
        self.mockRedirectComplexServiceGSecure(gamma)
        time.sleep(0.05)
        beta.serviceAll()
        time.sleep(0.05)

    self.assertEqual(len(beta.connector.rxbs), 0)
    self.assertIs(beta.waited, False)
    self.assertIs(beta.respondent.ended, True)

    self.assertEqual(len(beta.responses), 1)
    response = beta.responses.popleft()
    self.assertEqual(response, {'version': (1, 1),
                                'status': 200,
                                'reason': 'OK',
                                'headers':
                                    {'content-length': '122',
                                    'content-type': 'application/json',
                                    'date': 'Thu, 30 Apr 2015 19:37:17 GMT',
                                    'server': 'IoBook.local'},
                                'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                                b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                                b'on": null}'),
                                'data': {'action': None,
                                         'content': None,
                                         'query': {'name': 'fame'},
                                         'url': 'http://127.0.0.1:8080/echo?name=fame',
                                         'verb': 'GET'},
                                'error': None,
                                'errored': False,
                                'redirects': [{'body': bytearray(b''),
                                               'data': None,
                                               'headers': {'access-control-allow-origin': '*',
                                                           'content-length': '0',
                                                           'content-type': 'text/plain',
                                                           'location': 'https://localhost:6103/redirect?name=fame'},
                                               'reason': 'Temporary Redirect',
                                               'error': None,
                                               'errored': False,
                                               'request': {'body': b'',
                                                           'data': None,
                                                           'fargs': None,
                                                           'fragment': '',
                                                           'headers': {'accept': 'application/json'},
                                                           'host': 'localhost',
                                                           'method': 'GET',
                                                           'path': '/echo',
                                                           'port': 6101,
                                                           'qargs': {'name': 'fame'},
                                                           'scheme': 'https'},
                                               'status': 307,
                                               'version': (1, 1)}],
                                'request':
                                    {'host': 'localhost',
                                     'port': 6103,
                                     'scheme': 'https',
                                     'method': 'GET',
                                     'path': '/redirect',
                                     'qargs': {'name': 'fame'},
                                     'fragment': '',
                                     'headers':
                                         {'accept': 'application/json'},
                                     'body': b'',
                                     'data': None,
                                     'fargs': None,
                                    }
                                })

    alpha.close()
    gamma.close()
    beta.connector.close()

    wireLogAlpha.close()
    wireLogGamma.close()
    wireLogBeta.close()


def testMultiPartForm():
    """
    Test multipart form for Requester
    """
    console.terse("{0}\n".format(self.testMultiPartForm.__doc__))



    console.terse("{0}\n".format("Building Request ...\n"))
    host = u'127.0.0.1'
    port = 6101
    method = u'POST'
    path = u'/echo?name=fame'
    console.terse("{0} from  {1}:{2}{3} ...\n".format(method, host, port, path))
    headers = dict([(u'Accept', u'application/json'),
                     (u'Content-Type', u'multipart/form-data')])
    fargs = dict([("text",  "This is the life,\nIt is the best.\n"),
                   ("html", "<html><body></body><html>")])
    request =  clienting.Requester(hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers)
    msgOut = request.rebuild(fargs=fargs)

    self.assertTrue(b'Content-Disposition: form-data; name="text"\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nThis is the life,\nIt is the best.\n\r\n' in msgOut)
    self.assertTrue(b'Content-Disposition: form-data; name="html"\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n<html><body></body><html>\r\n' in msgOut)
    self.assertTrue(request.head.startswith(b'POST /echo?name=fame HTTP/1.1\r\nHost: 127.0.0.1:6101\r\nAccept-Encoding: identity\r\nContent-Length: 325\r\nAccept: application/json\r\nContent-Type: multipart/form-data; boundary='))



def testQueryQuoting():
    """
    Test agorithm for parsing and reassembling query
    """
    console.terse("{0}\n".format(self.testQueryQuoting.__doc__))


    location = u'https%3A%2F%2Fapi.twitter.com%2F1.1%2Faccount%2Fverify_credentials.json?oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU&oauth_nonce=eb616fe02004000&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1437580412&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD&oauth_version=1.0&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D'
    path, sep, query = location.partition('?')
    path = unquote(path)
    if sep:
        location = sep.join([path, query])
    else:
        location = path

    #location = unquote(location)
    self.assertEqual(location, u'https://api.twitter.com/1.1/account/verify_credentials.json?oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU&oauth_nonce=eb616fe02004000&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1437580412&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD&oauth_version=1.0&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')

    splits = urlsplit(location)
    query = splits.query
    self.assertEqual(query, u'oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU&oauth_nonce=eb616fe02004000&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1437580412&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD&oauth_version=1.0&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')
    querySplits = query.split(u'&')
    self.assertEqual(querySplits, [u'oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU',
                                    u'oauth_nonce=eb616fe02004000',
                                    u'oauth_signature_method=HMAC-SHA1',
                                    u'oauth_timestamp=1437580412',
                                    u'oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD',
                                    u'oauth_version=1.0',
                                    u'oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D'])
    qargs = dict()
    for queryPart in querySplits:  # this prevents duplicates even if desired
        if queryPart:
            if '=' in queryPart:
                key, val = queryPart.split('=', 1)
                val = unquote(val)
            else:
                key = queryPart
                val = True
            qargs[key] = val

    self.assertEqual(qargs, {u'oauth_consumer_key': u'meWtb1jEOCQciCgqheqiQoU',
                             u'oauth_nonce': u'eb616fe02004000',
                             u'oauth_signature_method': u'HMAC-SHA1',
                             u'oauth_timestamp': u'1437580412',
                             u'oauth_token': u'1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD',
                             u'oauth_version': u'1.0',
                             u'oauth_signature': u'KBD3DdNVZBjyOd0fqQ9X17ack='})
    qargParts = [u"{0}={1}".format(key, quote_plus(str(val)))
                 for key, val in qargs.items()]
    newQuery = '&'.join(qargParts)
    self.assertEqual(newQuery, u'oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU&oauth_nonce=eb616fe02004000&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1437580412&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD&oauth_version=1.0&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')



if __name__ == '__main__':
    test_client_redirect_simple()
