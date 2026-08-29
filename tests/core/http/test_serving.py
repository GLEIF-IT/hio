# -*- coding: utf-8 -*-
"""
Tests for http serving module
"""
import sys
import os
import time

import pytest

from hio import help
from hio.help import helping
from hio.base import tyming, doing
from hio.core import http, tcp
from hio.core.http import httping, serving


logger = help.ogler.getLogger()

tlsdirpath = os.path.dirname(
                os.path.dirname(
                        os.path.abspath(
                            sys.modules.get(__name__).__file__)))
certdirpath = os.path.join(tlsdirpath, 'tls', 'certs')


def _requestant_waiting_for_chunk_body():
    """Create a real Requestant paused after parsing chunked request headers."""
    remoter = tcp.Remoter(ha=("127.0.0.1", 6101),
                          ca=("127.0.0.1", 6102),
                          cs=None)
    requestant = serving.Requestant(
        msg=bytearray(b"POST / HTTP/1.1\r\n"
                      b"Host: localhost\r\n"
                      b"Transfer-Encoding: chunked\r\n\r\n"),
        remoter=remoter)
    requestant.parse()
    assert requestant.headed
    assert not requestant.bodied
    assert requestant.parser is not None
    return requestant


def _service_requestant(requestant, limit=8):
    """Drive a finite request parser without hiding a terminal EOF stall."""
    for _ in range(limit):
        requestant.parse()
        if requestant.parser is None:
            return
    raise AssertionError("request parser did not settle")


@pytest.mark.parametrize("started", [False, True])
def test_requestant_empty_eof_settles_without_request(started):
    """Empty EOF remains terminal without inventing an HTTP request."""
    remoter = tcp.Remoter(ha=("127.0.0.1", 6101),
                          ca=("127.0.0.1", 6102),
                          cs=None)
    requestant = serving.Requestant(msg=bytearray(), remoter=remoter)
    if started:
        # Suspend the parser while it awaits the first request byte.
        requestant.parse()
        assert requestant.parser is not None

    # An inbound peer may close cleanly without submitting a request.
    requestant.close()
    _service_requestant(requestant)

    assert requestant.closed
    assert requestant.ended
    assert not requestant.errored
    assert not requestant.headed
    assert not requestant.bodied


def test_wsgi_server_does_not_admit_empty_eof():
    """A settled headless request parser never creates a WSGI responder."""
    # Any entry proves that the server admitted a nonexistent HTTP request.
    calls = []

    def app(environ, start_response):
        calls.append(environ)
        return []

    with tcp.openServer(ha=("127.0.0.1", 0)) as servant:
        servant.eha = servant.ha
        with tcp.openClient(ha=servant.ha) as client:
            for _ in range(100):
                client.serviceConnect()
                servant.serviceConnects()
                if client.connected and client.ca in servant.ixes:
                    break
                time.sleep(0.01)
            assert client.connected
            assert client.ca in servant.ixes

            server = serving.Server(app=app,
                                    servant=servant,
                                    ha=servant.ha)
            remoter = servant.ixes[client.ca]
            requestant = serving.Requestant(msg=remoter.rxbs,
                                             remoter=remoter)
            # Model transport EOF before the client sends an HTTP start line.
            requestant.close()
            server.reqs[client.ca] = requestant

            server.serviceReqs()

            assert requestant.parser is None
            assert requestant.ended
            assert not requestant.errored
            assert not requestant.headed
            assert not calls
            assert not server.reps


def test_requestant_chunked_eof_consumes_terminal_zero_chunk():
    """Buffered request chunks remain incomplete until zero framing is read."""
    requestant = _requestant_waiting_for_chunk_body()
    requestant.msg.extend(b"4\r\nWiki\r\n0\r\n\r\n")
    requestant.close()

    _service_requestant(requestant)

    assert requestant.closed
    assert requestant.ended
    assert not requestant.errored
    assert requestant.bodied
    assert requestant.body == b"Wiki"
    assert not requestant.msg


@pytest.mark.parametrize(
    "body",
    [
        b"4\r\nWiki\r\n",
        b"4\r\nWiki\r\n0\r\nTrailer: value\r\n",
    ],
)
def test_requestant_chunked_eof_rejects_incomplete_terminator(body):
    """EOF after data cannot replace the zero chunk and complete trailers."""
    requestant = _requestant_waiting_for_chunk_body()
    requestant.msg.extend(body)
    requestant.close()

    _service_requestant(requestant)

    assert requestant.closed
    assert requestant.ended
    assert requestant.errored
    assert "closed unexpectedly" in requestant.error.lower()


@pytest.mark.parametrize(
    "initial, buffered, expected_body",
    [
        (b"",
         b"GET /buffered HTTP/1.1\r\nHost: localhost\r\n\r\n",
         b""),
        (b"GET /buffered HTTP/1.1\r\n",
         b"Host: localhost\r\n\r\n",
         b""),
        (b"POST /buffered HTTP/1.1\r\nHost: localhost\r\n"
         b"Content-Length: 4\r\n\r\n",
         b"test",
         b"test"),
    ],
)
def test_requestant_consumes_complete_buffered_framing_before_eof(
        initial, buffered, expected_body):
    """Complete start-line, headers, or fixed body buffered at EOF are valid."""
    remoter = tcp.Remoter(ha=("127.0.0.1", 6101),
                          ca=("127.0.0.1", 6102),
                          cs=None)
    requestant = serving.Requestant(msg=bytearray(initial), remoter=remoter)
    requestant.parse()
    assert requestant.parser is not None

    requestant.msg.extend(buffered)
    requestant.close()
    _service_requestant(requestant)

    assert requestant.closed
    assert requestant.ended
    assert not requestant.errored
    assert requestant.headed
    assert requestant.bodied
    assert requestant.path == "/buffered"
    assert requestant.body == expected_body
    assert not requestant.msg


def test_responder_content_length_closes_producer():
    """Content-Length completion closes rather than resumes the producer."""
    # Responder writes outbound HTTP bytes to an accepted TCP/TLS connection
    # through tx(). This stand-in captures that transport queue without adding
    # socket scheduling to a producer-lifecycle test.
    class Incomer:
        def __init__(self):
            self.txbs = bytearray()

        def tx(self, msg):
            self.txbs.extend(msg)

    events = []

    # HIO's Server is configured with a WSGI application like this generator.
    # Responder calls it with environ and start_response, then service() drives
    # the returned response-body iterable. Parsent is not involved here.
    def app(environ, start_response):
        try:
            start_response("200 OK", [("Content-Length", "4")])
            yield b"body"
            events.append("resumed")  # must not resume after the framed body
        finally:
            # Closing the WSGI producer must still release its resources.
            events.append("closed")

    incomer = Incomer()
    responder = serving.Responder(incomer=incomer,
                                  app=app,
                                  environ={},
                                  chunkable=True)

    responder.service()

    assert responder.size == 4
    assert responder.ended
    assert responder.iterator is None
    assert events == ["closed"]
    assert incomer.txbs.endswith(b"body")
    assert incomer.txbs.count(b"body") == 1


def _make_chunked_responder(values=(b"body", b"later")):
    """Build a production Responder over a real Remoter transmit queue."""
    remoter = tcp.Remoter(ha=("127.0.0.1", 6101),
                          ca=("127.0.0.1", 6102),
                          cs=None)

    def app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return values

    responder = serving.Responder(incomer=remoter,
                                  app=app,
                                  environ={},
                                  chunkable=True)
    return responder, remoter


class _CloseTrackingIterator:
    """
    Iterator distinct from its application-returned iterable.
    Models the iterator Responder obtains from iter(iterable).
    """

    def __init__(self, values):
        self.values = iter(values)
        self.next_count = 0
        self.close_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.next_count += 1
        return next(self.values)

    def close(self):
        self.close_count += 1


class _CloseTrackingIterable:
    """
    Application result with observable cleanup ownership.
    Models a closeable iterable returned by a WSGI application.
    """

    def __init__(self, iterator, close_error=None):
        self._iterator = iterator
        self.close_error = close_error
        self.close_count = 0

    def __iter__(self):
        return self._iterator

    def close(self):
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error


def _make_tracked_responder(values, headers=None, close_error=None):
    """Build a Responder whose returned iterable differs from its iterator."""
    iterator = _CloseTrackingIterator(values)
    iterable = _CloseTrackingIterable(iterator, close_error=close_error)
    remoter = tcp.Remoter(ha=("127.0.0.1", 6103),
                          ca=("127.0.0.1", 6104),
                          cs=None)

    # The WSGI application returns the iterable that owns cleanup.
    def app(environ, start_response):
        start_response("200 OK", headers or [("Content-Type", "text/plain")])
        return iterable

    responder = serving.Responder(incomer=remoter,
                                  app=app,
                                  environ={},
                                  chunkable=True)
    return responder, remoter, iterable, iterator


def test_responder_closes_returned_iterable_after_normal_exhaustion():
    """Normal WSGI exhaustion closes the application result exactly once."""
    # Arrange: return an iterable whose iterator is a different closeable object.
    responder, remoter, iterable, iterator = _make_tracked_responder([b"body"])

    # Act: emit the body, observe exhaustion, and reset for persistent reuse.
    responder.service()
    responder.service()
    responder.reset(environ={"request": "next"}, chunkable=True)

    # Assert: cleanup belongs to the returned iterable and is not repeated.
    assert iterable.close_count == 1
    assert iterator.close_count == 0
    assert responder.iterable is None
    assert responder.iterator is None
    assert remoter.txbs.count(b"0\r\n\r\n") == 1


def test_responder_content_length_closes_returned_iterable():
    """Early Content-Length completion closes the application result."""
    # Arrange: leave another value behind the exact declared response length.
    responder, _, iterable, iterator = _make_tracked_responder(
        [b"body", b"later"], headers=[("Content-Length", "4")])

    # Act: one service call reaches the exact HTTP body boundary.
    responder.service()

    # Assert: HIO stops iteration and closes the returned owner, not its iterator.
    assert responder.ended
    assert iterator.next_count == 1
    assert iterable.close_count == 1
    assert iterator.close_count == 0
    assert responder.iterable is None
    assert responder.iterator is None


@pytest.mark.parametrize("operation", ["close", "abort"])
def test_responder_failure_closes_returned_iterable_once(operation):
    """Every incomplete terminal path releases the WSGI application result."""
    # Arrange: partially emit a response while the producer still has work.
    responder, remoter, iterable, iterator = _make_tracked_responder(
        [b"body", b"later"])
    responder.service()
    before = bytes(remoter.txbs)

    # Act: terminate through connection closure or an explicit producer abort.
    if operation == "close":
        responder.close()
        responder.close()
    else:
        responder.abort(BrokenPipeError("response transport closed"))
        responder.close()

    # Assert: failure adds no framing and closes only the application result.
    assert responder.closed
    assert responder.errored
    assert not responder.ended
    assert bytes(remoter.txbs) == before
    assert iterable.close_count == 1
    assert iterator.close_count == 0
    assert responder.iterable is None
    assert responder.iterator is None


def test_responder_iterable_cleanup_failure_prevents_completion():
    """Cleanup failure wins before successful terminal framing is claimed."""
    # Arrange: make returned-iterable cleanup fail after its body is exhausted.
    error = RuntimeError("iterable cleanup failed")
    responder, remoter, iterable, iterator = _make_tracked_responder(
        [b"body"], close_error=error)
    responder.service()
    before = bytes(remoter.txbs)

    # Act: observe exhaustion and the resulting cleanup failure.
    responder.service()

    # Assert: the cleanup cause is retained and no terminal chunk is appended.
    assert responder.closed
    assert responder.errored
    assert responder.error is error
    assert not responder.ended
    assert bytes(remoter.txbs) == before
    assert not remoter.txbs.endswith(b"0\r\n\r\n")
    assert iterable.close_count == 1
    assert iterator.close_count == 0
    assert responder.iterable is None
    assert responder.iterator is None


@pytest.mark.parametrize("after_output", [False, True])
def test_responder_incomplete_close_is_failure(after_output):
    """Administrative close cannot impersonate normal WSGI exhaustion."""
    # Arrange: cover closure before response start and after partial output.
    responder, remoter = _make_chunked_responder()
    if after_output:
        responder.service()
    before = bytes(remoter.txbs)

    # Act: administratively close the incomplete response.
    responder.close()

    # Assert: teardown records failure without synthesizing terminal framing.
    assert responder.closed
    assert responder.errored
    assert isinstance(responder.error, httping.PrematureClosure)
    assert not responder.ended
    assert bytes(remoter.txbs) == before
    assert not remoter.txbs.endswith(b"0\r\n\r\n")

    # Act and assert: repeated close retains the cause and byte boundary.
    error = responder.error
    responder.close()
    assert responder.error is error
    assert bytes(remoter.txbs) == before


def test_responder_abort_retains_first_failure():
    """Abort is idempotent and preserves its first causal exception."""
    # Arrange: partially emit a response before transport failure.
    responder, remoter = _make_chunked_responder()
    responder.service()
    before = bytes(remoter.txbs)
    error = BrokenPipeError("response transport closed")

    # Act: abort with the failure that ended production.
    responder.abort(error)

    # Assert: failure settles without claiming completion or adding bytes.
    assert responder.closed
    assert responder.errored
    assert responder.error is error
    assert not responder.ended
    assert bytes(remoter.txbs) == before

    # Act and assert: a later abort cannot obscure the first cause.
    responder.abort(RuntimeError("later failure"))
    assert responder.error is error
    assert bytes(remoter.txbs) == before


def test_responder_failure_cannot_be_reused():
    """Reset accepts only a normally completed responder generation."""
    # Arrange: settle the response generation as failed.
    responder, _ = _make_chunked_responder()
    responder.close()

    # Act and assert: persistent reuse cannot reclassify that failure.
    with pytest.raises(RuntimeError, match="without normal completion"):
        responder.reset(environ={"request": "next"}, chunkable=True)


def test_responder_normal_completion_remains_successful():
    """Normal exhaustion still emits one terminal chunk before close."""
    # Arrange: use a finite producer with one body chunk.
    responder, remoter = _make_chunked_responder(values=(b"body",))

    # Act: advance once for the body and once to observe exhaustion.
    responder.service()
    responder.service()

    # Assert: exhaustion owns successful terminal framing.
    assert responder.ended
    assert not responder.closed
    assert remoter.txbs.endswith(b"4\r\nbody\r\n0\r\n\r\n")

    # Act and assert: later close preserves success and adds no framing.
    responder.close()
    assert responder.ended
    assert responder.closed
    assert remoter.txbs.count(b"0\r\n\r\n") == 1


def test_wsgi_server_reuse_resets_request_scoped_response_state():
    """A reused responder derives transfer state from the next request."""
    ca = ("127.0.0.1", 6101)
    # Use the production connection type without opening a socket; no I/O occurs.
    remoter = tcp.Remoter(ha=("127.0.0.1", 6100), ca=ca, cs=None)

    # Model the next parsed HTTP/1.1 request on the same connection.
    class Requestant:
        parser = True
        ended = False
        errored = False
        error = None
        headed = True
        method = "GET"
        path = "/next"
        version = (1, 1)
        headers = help.Hict()
        body = bytearray()

        def parse(self):
            self.parser = None
            self.ended = True

    # Seed response-scoped state left by the prior HTTP/1.1 SSE response.
    responder = serving.Responder(incomer=remoter,
                                  app=None,
                                  environ={"request": "old"},
                                  chunkable=True)
    responder.start("200 OK", [("Content-Type", "text/event-stream")])
    responder.ended = True
    assert responder.evented
    assert responder.chunkable

    requestant = Requestant()
    requestant.remoter = remoter
    # Isolate the Server handoff that reuses the existing Responder.
    server = serving.Server(app=None, port=6101)
    server.reqs[ca] = requestant
    server.reps[ca] = responder
    server.buildEnviron = lambda request: {"request": "next"}

    server.serviceReqs()  # calls responder.reset, passing in "chunkable" during reset

    assert responder.environ == {"request": "next"}
    assert responder.chunkable  # should have received this from the .reset call
    assert not responder.evented

    responder.start("200 OK", [("Content-Type", "text/plain")])
    head = responder.build()
    assert b"Transfer-Encoding: chunked\r\n" in head  # verifies passed in "chunked" is used


def test_bare_server_echo():
    """
    Test BaserServer service request response of echo non blocking
    """
    tymist = tyming.Tymist(tyme=0.0)

    with http.openServer(cls=http.BareServer, port = 6101, bufsize=131072, \
                         tymth=tymist.tymen()) as alpha:

        assert alpha.servant.ha == ('0.0.0.0', 6101)
        assert alpha.servant.eha == ('127.0.0.1', 6101)


        path = "http://{0}:{1}/".format('localhost', alpha.servant.eha[1])
        with http.openClient(bufsize=131072, path=path, tymth=tymist.tymen(), \
                             reconnectable=True,) as  beta:

            assert not beta.connector.accepted
            assert not beta.connector.connected
            assert not beta.connector.cutoff

            request = dict([('method', u'GET'),
                             ('path', u'/echo?name=fame'),
                             ('qargs', dict()),
                             ('fragment', u''),
                             ('headers', dict([('Accept', 'application/json'),
                                                ('Content-Length', 0)])),
                            ])

            beta.requests.append(request)

            while (beta.requests or beta.connector.txbs or not beta.responses or
                   not alpha.servant.ixes or not alpha.idle()):
                alpha.service()
                time.sleep(0.05)
                beta.service()
                time.sleep(0.05)

            assert beta.connector.accepted
            assert beta.connector.connected
            assert not beta.connector.cutoff

            assert len(alpha.servant.ixes) == 1
            assert len(alpha.stewards) == 1
            requestant = list(alpha.stewards.values())[0].requestant
            assert requestant.method == request['method']
            assert requestant.url == request['path']
            assert requestant.headers == help.Hict([('Host', 'localhost:6101'),
                                                         ('Accept-Encoding', 'identity'),
                                                         ('Accept', 'application/json'),
                                                         ('Content-Length', '0')])

            assert len(beta.responses) == 1
            response = beta.responses.popleft()
            assert response['data'] == {'version': 'HTTP/1.1',
                                        'method': 'GET',
                                        'path': '/echo',
                                        'qargs': {'name': 'fame'},
                                        'fragment': '',
                                        'headers': [['Host', 'localhost:6101'],
                                                    ['Accept-Encoding', 'identity'],
                                                    ['Accept', 'application/json'],
                                                    ['Content-Length', '0']],
                                        'body': '',
                                        'data': None}

            responder = list(alpha.stewards.values())[0].responder
            assert responder.status == response['status']
            assert responder.headers == response['headers']


def test_wsgi_server():
    """
    Test WSGI Server service request response
    """
    tymist = tyming.Tymist(tyme=0.0)

    def wsgiApp(environ, start_response):
        start_response('200 OK', [('Content-type','text/plain'),
                                  ('Content-length', '12')])
        return [b"Hello World!"]

    with http.openServer(port = 6101, bufsize=131072, app=wsgiApp, \
                         tymth=tymist.tymen()) as alpha:  # passthrough

        assert alpha.servant.ha == ('0.0.0.0', 6101)
        assert alpha.servant.eha == ('127.0.0.1', 6101)

        path = "http://{0}:{1}/".format('localhost', alpha.servant.eha[1])

        with http.openClient(bufsize=131072, path=path, reconnectable=True, \
                             tymth=tymist.tymen()) as beta:

            assert not beta.connector.accepted
            assert not beta.connector.connected
            assert not beta.connector.cutoff

            request = dict([('method', u'GET'),
                             ('path', u'/echo?name=fame'),
                             ('qargs', dict()),
                             ('fragment', u''),
                             ('headers', dict([('Accept', 'application/json'),
                                                ('Content-Length', 0)])),
                            ])

            beta.requests.append(request)

            while (beta.requests or beta.connector.txbs or not beta.responses or
                   not alpha.idle()):
                alpha.service()
                time.sleep(0.05)
                beta.service()
                time.sleep(0.05)

            assert beta.connector.accepted
            assert beta.connector.connected
            assert not beta.connector.cutoff

            assert len(alpha.servant.ixes) == 1
            assert len(alpha.reqs) == 1
            assert len(alpha.reps) == 1
            requestant = list(alpha.reqs.values())[0]
            assert requestant.method == request['method']
            assert requestant.url == request['path']
            assert requestant.headers == help.Hict([('Host', 'localhost:6101'),
                                                         ('Accept-Encoding', 'identity'),
                                                         ('Accept', 'application/json'),
                                                         ('Content-Length', '0')])


            assert len(beta.responses) == 1
            response = beta.responses.popleft()
            assert response['body'] == (b'Hello World!')
            assert response['status'] == 200

            responder = list(alpha.reps.values())[0]
            assert responder.status.startswith(str(response['status']))
            assert responder.headers == response['headers']


def test_wsgi_server_tls():
    """
    Test Valet WSGI service with secure TLS request response
    """
    tymist = tyming.Tymist(tyme=0.0)

    def wsgiApp(environ, start_response):
        start_response('200 OK', [('Content-type','text/plain'),
                                  ('Content-length', '12')])
        return [b"Hello World!"]

    serverCertCommonName = 'localhost' # match hostname uses servers's cert commonname
    #serverKeypath = '/etc/pki/tls/certs/server_key.pem'  # local server private key
    #serverCertpath = '/etc/pki/tls/certs/server_cert.pem'  # local server public cert
    #clientCafilepath = '/etc/pki/tls/certs/client.pem' # remote client public cert

    serverKeypath = certdirpath + '/server_key.pem'  # local server private key
    serverCertpath = certdirpath + '/server_cert.pem'  # local server public cert
    clientCafilepath = certdirpath + '/client.pem' # remote client public cert

    with http.openServer(port = 6101, bufsize=131072, app=wsgiApp, \
                         scheme='https', keypath=serverKeypath, \
                         certpath=serverCertpath, cafilepath=clientCafilepath, \
                         tymth=tymist.tymen()) as alpha:

        assert alpha.servant.ha == ('0.0.0.0', 6101)
        assert alpha.servant.eha == ('127.0.0.1', 6101)

        #clientKeypath = '/etc/pki/tls/certs/client_key.pem'  # local client private key
        #clientCertpath = '/etc/pki/tls/certs/client_cert.pem'  # local client public cert
        #serverCafilepath = '/etc/pki/tls/certs/server.pem' # remote server public cert

        clientKeypath = certdirpath + '/client_key.pem'  # local client private key
        clientCertpath = certdirpath + '/client_cert.pem'  # local client public cert
        serverCafilepath = certdirpath + '/server.pem' # remote server public cert

        path = "https://{0}:{1}/".format('localhost', alpha.servant.eha[1])

        with http.openClient(bufsize=131072, path=path, scheme='https', \
                    certedhost=serverCertCommonName, keypath=clientKeypath, \
                    certpath=clientCertpath, cafilepath=serverCafilepath, \
                    tymth=tymist.tymen(), reconnectable=True,) as beta:

            assert not beta.connector.accepted
            assert not beta.connector.connected
            assert not beta.connector.cutoff

            request = dict([('method', u'GET'),
                             ('path', u'/echo?name=fame'),
                             ('qargs', dict()),
                             ('fragment', u''),
                             ('headers', dict([('Accept', 'application/json'),
                                                ('Content-Length', 0)])),
                            ])

            beta.requests.append(request)

            while (beta.requests or beta.connector.txbs or not beta.responses or
                   not alpha.idle()):
                alpha.service()
                time.sleep(0.05)
                beta.service()
                time.sleep(0.05)

            assert beta.connector.accepted
            assert beta.connector.connected
            assert not beta.connector.cutoff

            assert len(alpha.servant.ixes) == 1
            assert len(alpha.reqs) == 1
            assert len(alpha.reps) == 1
            requestant = list(alpha.reqs.values())[0]
            assert requestant.method == request['method']
            assert requestant.url == request['path']
            assert requestant.headers == help.Hict([('Host', 'localhost:6101'),
                                                         ('Accept-Encoding', 'identity'),
                                                         ('Accept', 'application/json'),
                                                         ('Content-Length', '0')])

            assert len(beta.responses) == 1
            response = beta.responses.popleft()
            assert response['body'] == (b'Hello World!')
            assert response['status'] == 200

            responder = list(alpha.reps.values())[0]
            responder.status.startswith(str(response['status']))
            assert responder.headers == response['headers']


def test_server_client_doers():
    """
    Test HTTP ServerDoer ClientDoer classes
    """
    tock = 0.03125
    ticks = 16
    limit = ticks * tock
    doist = doing.Doist(tock=tock, real=True, limit=limit)
    assert doist.tyme == 0.0  # on next cycle
    assert doist.tock == tock == 0.03125
    assert doist.real == True
    assert doist.limit == limit == 0.5
    assert doist.doers == []

    def wsgiApp(environ, start_response):
        start_response('200 OK', [('Content-type','text/plain'),
                              ('Content-length', '12')])
        return [b"Hello World!"]

    port = 6101
    server = http.Server(port=port, app=wsgiApp, tymth=doist.tymen())
    assert server.servant.tyme == doist.tyme

    serdoer = http.ServerDoer(tymth=doist.tymen(), server=server)
    assert serdoer.server ==  server
    assert serdoer.tyme ==  serdoer.server.servant.tyme == doist.tyme

    path = "http://{0}:{1}/".format('localhost', port)
    client = http.Client(path=path, tymth=doist.tymen())
    assert client.connector.tyme == doist.tyme

    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json'),
                                        ('Content-Length', 0)])),
                    ])

    client.requests.append(request)

    clidoer = http.ClientDoer(tymth=doist.tymen(), client=client)
    assert clidoer.client == client
    assert clidoer.tyme == clidoer.client.connector.tyme == doist.tyme

    assert serdoer.tock == 0.0  # ASAP
    assert clidoer.tock == 0.0  # ASAP

    doers = [serdoer, clidoer]

    doist.do(doers=doers, limit=limit)
    assert doist.tyme == limit
    assert server.servant.opened == False
    assert client.connector.opened == False

    assert len(client.responses) == 1
    response = client.responses.popleft()
    assert response['body'] == (b'Hello World!')
    assert response['status'] == 200
    """End Test """


if __name__ == '__main__':
    test_server_client_doers()
