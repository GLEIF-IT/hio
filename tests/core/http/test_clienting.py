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
from hio.help import Hict
from hio.base import tyming
from hio.core import wiring, tcp, http
from hio.core.http import clienting


logger = help.ogler.getLogger()

tlsdirpath = os.path.dirname(
                os.path.dirname(
                        os.path.abspath(
                            sys.modules.get(__name__).__file__)))
certdirpath = os.path.join(tlsdirpath, 'tls', 'certs')


def _service_client_connection(client, server):
    """Drive a real nonblocking Client connection through server acceptance."""
    for _ in range(100):
        client.connector.serviceConnect()
        server.serviceConnects()
        if (client.connector.connected and
                client.connector.ca in server.ixes):
            return
        time.sleep(0.01)
    raise AssertionError("client did not connect")


def _deliver_response_and_cutoff(client, server, payload):
    """Complete a real request, deliver a response, and close its server ix."""
    # Start an exchange on the current transport generation.
    client.transmit(method="GET", path="/old-generation")
    ca = client.connector.ca
    remoter = server.ixes[ca]

    # Deliver the complete request to the real server-side connection.
    for _ in range(100):
        client.connector.serviceSends()
        server.serviceReceivesAllIx()
        if not client.connector.txbs and remoter.rxbs:
            break
        time.sleep(0.01)
    assert not client.connector.txbs
    assert remoter.rxbs
    remoter.clearRxbs()

    # Buffer the response at the client before the peer closes its socket.
    remoter.tx(payload)
    for _ in range(100):
        remoter.serviceSends()
        client.connector.serviceReceives()
        if payload in client.connector.rxbs:
            break
        time.sleep(0.01)
    assert payload in client.connector.rxbs

    # Retiring the server ix produces peer EOF and client-side cutoff.
    server.removeIx(ca)
    for _ in range(100):
        client.connector.serviceReceives()
        if client.connector.cutoff:
            break
        time.sleep(0.01)
    assert client.connector.cutoff
    return client.connector.cs


@pytest.mark.parametrize(
    "connector_cls, scheme, port",
    [
        (tcp.Client, "http", 56000),
        (tcp.ClientTls, "https", 56001),
    ],
)
def test_client_exposes_connector_lifecycle(connector_cls, scheme, port):
    """HTTP Client exposes its raw or TLS connector's directional facts."""
    connector = connector_cls(host="127.0.0.1", port=port)
    client = clienting.Client(connector=connector, scheme=scheme)

    assert not client.cutoff
    assert not client.txCutoff
    assert client.error is None

    connector.cutoff = True
    assert client.cutoff
    assert not client.txCutoff

    connector.cutoff = False
    connector.txCutoff = True
    error = BrokenPipeError("send direction closed")
    connector.error = error
    assert not client.cutoff
    assert client.txCutoff
    assert client.error is error

    assert client.reopen()
    assert not client.cutoff
    assert not client.txCutoff
    assert client.error is None

    client.close()


@pytest.mark.parametrize(
    "payload, expected_body, expected_error",
    [
        (b"HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nold-body",
         b"old-body",
         False),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nab",
         b"",
         True),
    ],
)
def test_client_cutoff_settles_old_response_before_reconnect(
        payload, expected_body, expected_error):
    """Reconnect cannot erase a complete or failed old response generation."""
    tymist = tyming.Tymist(tyme=0.0)
    with tcp.openServer(tymth=tymist.tymen(),
                        ha=("127.0.0.1", 0)) as server:
        server.eha = server.ha
        with clienting.openClient(hostname=server.ha[0],
                                  port=server.ha[1],
                                  tymth=tymist.tymen(),
                                  reconnectable=True,
                                  tymeout=1.0) as client:
            _service_client_connection(client, server)
            old_socket = _deliver_response_and_cutoff(client, server, payload)
            tymist.tick(tock=1.1)
            assert client.connector.tymer.expired

            # Settle the buffered response, then open the replacement generation.
            client.service()

            # Reconnectability replaces a dead socket; it does not reuse it.
            assert old_socket.fileno() == -1
            assert not client.waited
            assert len(client.responses) == 1
            response = client.responses.popleft()
            assert response["body"] == expected_body
            assert response["errored"] is expected_error
            if expected_error:
                assert "closed unexpectedly" in response["error"].lower()
            else:
                assert response["error"] is None
            assert not client.connector.rxbs
            assert client.connector.opened
            assert not client.connector.connected
            assert not client.respondent.closed
            assert not client.respondent.headed
            assert not client.respondent.ended


def test_client_cutoff_preserves_sse_retry_before_reconnect():
    """SSE cutoff parses accepted events and applies their retry interval."""
    payload = (b"HTTP/1.1 200 OK\r\n"
               b"Content-Type: text/event-stream\r\n\r\n"
               b"retry: 2500\n"
               b"id: event-1\n"
               b"data: old-event\n\n")
    tymist = tyming.Tymist(tyme=0.0)
    with tcp.openServer(tymth=tymist.tymen(),
                        ha=("127.0.0.1", 0)) as server:
        server.eha = server.ha
        with clienting.openClient(hostname=server.ha[0],
                                  port=server.ha[1],
                                  tymth=tymist.tymen(),
                                  reconnectable=True,
                                  tymeout=1.0) as client:
            _service_client_connection(client, server)
            old_socket = _deliver_response_and_cutoff(client, server, payload)
            tymist.tick(tock=1.1)

            # Parse old-generation SSE bytes before resetting the transport.
            client.service()

            # The physical socket is replaced while logical SSE state survives.
            assert old_socket.fileno() == -1
            assert client.waited
            assert client.respondent.evented
            assert client.respondent.retry == 2500
            assert client.connector.tymer.duration == 2.5
            assert len(client.events) == 1
            assert client.events[0]["id"] == "event-1"
            assert client.events[0]["data"] == "old-event"
            assert not client.connector.rxbs
            assert client.connector.opened
            assert not client.connector.connected
            assert not client.respondent.closed
            assert not client.respondent.headed
            assert not client.respondent.ended


def test_respondent_reopen_resets_parser_generation():
    """A new transport generation explicitly resets terminal parser state."""
    respondent = clienting.Respondent(msg=bytearray())
    old_parser = respondent.parser
    respondent.started = True
    respondent.headed = True
    respondent.bodied = True
    respondent.ended = True
    respondent.closed = True
    respondent.errored = True
    respondent.error = "old transport failed"

    respondent.reopen()

    assert respondent.parser is not old_parser
    assert not respondent.started
    assert not respondent.headed
    assert not respondent.bodied
    assert not respondent.ended
    assert not respondent.closed
    assert not respondent.errored
    assert respondent.error is None


def test_client_reopen_resets_parser_generation():
    """A legal manual reopen resets parsing and discards old receive bytes."""
    with tcp.openServer(ha=("127.0.0.1", 0)) as server:
        # Acceptor does not refresh .eha after binding an ephemeral port.
        server.eha = server.ha
        with clienting.openClient(hostname=server.ha[0],
                                  port=server.ha[1]) as client:
            _service_client_connection(client, server)
            old_socket = client.connector.cs
            old_parser = client.respondent.parser

            # Leave accepted old-generation bytes unclaimed by any exchange.
            remoter = server.ixes[client.connector.ca]
            remoter.tx(b"stale-old-generation")
            for _ in range(100):
                remoter.serviceSends()
                client.connector.serviceReceives()
                if client.connector.rxbs:
                    break
                time.sleep(0.01)
            assert client.connector.rxbs == b"stale-old-generation"
            client.respondent.close()

            assert client.reopen()  # should clear stale bytes on manual reopen

            assert old_socket.fileno() == -1
            assert client.connector.cs is not old_socket
            assert not client.connector.rxbs
            assert client.respondent.parser is not old_parser
            assert not client.respondent.closed


def test_client_reopen_rejects_active_exchange():
    """Manual reopen cannot discard an unsettled request and response."""
    with tcp.openServer(ha=("127.0.0.1", 0)) as server:
        server.eha = server.ha
        with clienting.openClient(hostname=server.ha[0],
                                  port=server.ha[1]) as client:
            _service_client_connection(client, server)

            # Queue but do not service the request, keeping the exchange active.
            client.transmit(method="GET", path="/active")
            old_socket = client.connector.cs
            old_parser = client.respondent.parser
            old_request = bytes(client.connector.txbs)

            with pytest.raises(RuntimeError, match="response is still active"):
                client.reopen()

            # Rejection must leave every active-generation owner unchanged.
            assert client.waited
            assert client.connector.cs is old_socket
            assert old_socket.fileno() != -1
            assert client.connector.connected
            assert client.respondent.parser is old_parser
            assert bytes(client.connector.txbs) == old_request


def test_client_automatic_reconnect_resets_parser_generation():
    """A real cutoff/reconnect uses the parser-generation transition."""
    tymist = tyming.Tymist(tyme=0.0)
    with tcp.openServer(tymth=tymist.tymen(),
                        ha=("127.0.0.1", 0)) as server:
        server.eha = server.ha
        with clienting.openClient(hostname=server.ha[0],
                                  port=server.ha[1],
                                  tymth=tymist.tymen(),
                                  reconnectable=True,
                                  tymeout=1.0) as client:
            _service_client_connection(client, server)
            old_parser = client.respondent.parser

            server.removeIx(client.connector.ca)  # trigger server EOF; client eventually gets EOF
            for _ in range(100):
                client.connector.serviceReceives()
                if client.connector.cutoff:
                    break
                time.sleep(0.01)
            assert client.connector.cutoff  # we've received EOF and should be cutoff

            tymist.tick(tock=1.1)
            assert client.connector.tymer.expired
            client.service()  # triggers new parser setup

            assert client.respondent.parser is not old_parser  # should have new parser
            assert not client.respondent.closed
            assert not client.connector.cutoff


def test_cross_origin_redirect_resets_parser_generation():
    """A real cross-origin connector replacement rearms response parsing."""
    with tcp.openServer(ha=("127.0.0.1", 0)) as origin, \
         tcp.openServer(ha=("127.0.0.1", 0)) as target:
        origin.eha = origin.ha
        target.eha = target.ha
        with clienting.openClient(hostname=origin.ha[0],
                                  port=origin.ha[1]) as client:
            _service_client_connection(client, origin)
            old_connector = client.connector
            old_socket = old_connector.cs
            old_parser = client.respondent.parser
            client.respondent.close()
            client.redirects = [{
                "headers": help.Hict({
                    "Location": "http://{}:{}/next".format(*target.ha),
                }),
                "method": "GET",
            }]

            client.redirect()

            assert old_socket.fileno() == -1
            assert client.connector is not old_connector
            assert isinstance(client.connector, tcp.Client)
            assert client.connector.ha == target.ha
            assert client.respondent.msg is client.connector.rxbs
            assert client.respondent.parser is not old_parser
            assert not client.respondent.closed
            _service_client_connection(client, target)


def _respondent_waiting_for_chunk_body():
    """Create a real Respondent paused after parsing chunked response headers."""
    respondent = clienting.Respondent(
        msg=bytearray(b"HTTP/1.1 200 OK\r\n"
                      b"Transfer-Encoding: chunked\r\n\r\n"))
    respondent.parse()
    assert respondent.headed
    assert not respondent.bodied
    assert respondent.parser is not None
    return respondent


def _service_respondent(respondent, limit=8):
    """Drive a finite response parser without hiding a terminal EOF stall."""
    for _ in range(limit):
        respondent.parse()
        if respondent.parser is None:
            return
    raise AssertionError("response parser did not settle")


@pytest.mark.parametrize("started", [False, True])
def test_respondent_empty_eof_settles_as_error(started):
    """Empty EOF remains terminal before or after the parser first runs."""
    respondent = clienting.Respondent(msg=bytearray())
    if started:
        # Suspend the parser while it awaits the first response byte.
        respondent.parse()
        assert respondent.parser is not None

    # The peer closes without sending even an HTTP status line.
    respondent.close()
    _service_respondent(respondent)

    assert respondent.closed
    assert respondent.ended
    assert respondent.errored
    assert "before HTTP message" in respondent.error


def test_respondent_chunked_eof_consumes_terminal_zero_chunk():
    """Buffered response chunks remain incomplete until zero framing is read."""
    respondent = _respondent_waiting_for_chunk_body()
    respondent.msg.extend(b"4\r\nWiki\r\n0\r\n\r\n")
    respondent.close()

    _service_respondent(respondent)

    assert respondent.closed
    assert respondent.ended
    assert not respondent.errored
    assert respondent.bodied
    assert respondent.body == b"Wiki"
    assert not respondent.msg


@pytest.mark.parametrize(
    "body",
    [
        b"4\r\nWiki\r\n",
        b"4\r\nWiki\r\n0\r\nTrailer: value\r\n",
    ],
)
def test_respondent_chunked_eof_rejects_incomplete_terminator(body):
    """EOF after data cannot replace the zero chunk and complete trailers."""
    respondent = _respondent_waiting_for_chunk_body()
    respondent.msg.extend(body)
    respondent.close()

    _service_respondent(respondent)

    assert respondent.closed
    assert respondent.ended
    assert respondent.errored
    assert "closed unexpectedly" in respondent.error.lower()


@pytest.mark.parametrize(
    "initial, buffered, expected_body",
    [
        (b"",
         b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
         b""),
        (b"HTTP/1.1 200 OK\r\n",
         b"Content-Length: 0\r\n\r\n",
         b""),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n",
         b"test",
         b"test"),
    ],
)
def test_respondent_consumes_complete_buffered_framing_before_eof(
        initial, buffered, expected_body):
    """Complete start-line, headers, or fixed body buffered at EOF are valid."""
    respondent = clienting.Respondent(msg=bytearray(initial))
    respondent.parse()
    assert respondent.parser is not None

    respondent.msg.extend(buffered)
    respondent.close()
    _service_respondent(respondent)

    assert respondent.closed
    assert respondent.ended
    assert not respondent.errored
    assert respondent.headed
    assert respondent.bodied
    assert respondent.body == expected_body
    assert not respondent.msg


@pytest.mark.parametrize(
    "initial, partial",
    [
        (b"", b"HTTP/1.1 200"),
        (b"HTTP/1.1 200 OK\r\n", b"Content-Length: 0\r\n"),
        (b"HTTP/1.1 100 Continue\r\n", b"Extension: partial"),
        (b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n", b"te"),
    ],
)
def test_respondent_rejects_partial_buffered_framing_at_eof(initial, partial):
    """Partial start-line, headers, or fixed body at EOF settle as failure."""
    respondent = clienting.Respondent(msg=bytearray(initial))
    respondent.parse()
    assert respondent.parser is not None

    respondent.msg.extend(partial)
    respondent.close()
    _service_respondent(respondent)

    assert respondent.closed
    assert respondent.ended
    assert respondent.errored
    assert "closed unexpectedly" in respondent.error.lower()


def mockEchoService(server):
    """
    mock echo server service for testing
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
               b'Host: 127.0.0.1:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Accept: application/json\r\n\r\n')

    mockMsgLocalhost = (b'GET /echo?name=fame HTTP/1.1\r\n'
                        b'Host: localhost:6101\r\n'
                        b'Accept-Encoding: identity\r\n'
                        b'Accept: application/json\r\n\r\n')

    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if  msgIn in (mockMsg, mockMsgLocalhost):
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


def mockRedirectPathService(server):
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


def mockRedirectingService(server):
    """
    mock redirecting echo server service utility function
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
               b'Host: 127.0.0.1:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Accept: application/json\r\n\r\n')

    mockMsgLocalhost = (b'GET /echo?name=fame HTTP/1.1\r\n'
                        b'Host: localhost:6101\r\n'
                        b'Accept-Encoding: identity\r\n'
                        b'Accept: application/json\r\n\r\n')


    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn in (mockMsg, mockMsgLocalhost):
            ixClient.clearRxbs()
            msgOut = (b'HTTP/1.1 307 Temporary Redirect\r\n'
                      b'Content-Type: text/plain\r\n'
                      b'Content-Length: 0\r\n'
                      b'Access-Control-Allow-Origin: *\r\n'
                      b'Location: http://localhost:6103/redirect?name=fame\r\n\r\n')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()


def mockRedirectedService(server):
    """
    mock redirected echo server service utility function
    """
    mockMsg = (b'GET /redirect?name=fame HTTP/1.1\r\n'
               b'Host: localhost:6103\r\n'
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


def mockRedirectingServiceTls(server):
    """
    mock redirecting echo server service utility function with
    redirected location is https  tls
    """
    mockMsg = (b'GET /echo?name=fame HTTP/1.1\r\n'
               b'Host: localhost:6101\r\n'
               b'Accept-Encoding: identity\r\n'
               b'Accept: application/json\r\n\r\n')

    server.serviceConnects()
    if server.ixes:
        server.serviceReceivesAllIx()
        if not server.ixes:  # truthful TLS truncation removes the connection
            return

        ixClient = list(server.ixes.values())[0]
        msgIn = bytes(ixClient.rxbs)
        if msgIn in (mockMsg,):
            ixClient.clearRxbs()
            # redirect location is https
            msgOut = (b'HTTP/1.1 307 Temporary Redirect\r\n'
                      b'Content-Type: text/plain\r\n'
                      b'Content-Length: 0\r\n'
                      b'Access-Control-Allow-Origin: *\r\n'
                      b'Location: https://localhost:6103/redirect?name=fame\r\n\r\n')
            ixClient.tx(msgOut)
            msgIn = b''
            msgOut = b''

        server.serviceSendsAllIx()



def test_client_request_echo():
    """
    Test HTTP Client request echo non blocking
    """
    with tcp.openServer(port = 6101, bufsize=131072) as alpha:

        assert alpha.ha == ('0.0.0.0', 6101)
        assert alpha.eha == ('127.0.0.1', 6101)

        host = alpha.eha[0]
        port = alpha.eha[1]
        method = u'GET'
        path = u'/echo?name=fame'
        headers = dict([('Accept', 'application/json')])

        with http.openClient(bufsize=131072, hostname=host, port=port, \
                    method=method, path=path, headers=headers,) as beta:

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


def test_client_request_echo_port_empty():
    """
    Test HTTP Client request echo non blocking with portOptional set to True
    Uses port 80 so may cause problems when testing on linux github

    """
    with tcp.openServer(port = 80, bufsize=131072) as alpha:
        if sys.platform == "linux" or sys.platform == "win32":
            assert alpha.ha == ('', 80)
            assert alpha.eha == ('127.0.0.1', 80)
        else:
            assert alpha.ha == ('0.0.0.0', 80)
            assert alpha.eha == ('127.0.0.1', 80)

        host = alpha.eha[0]
        port = alpha.eha[1]
        method = u'GET'
        path = u'/echo?name=fame'
        headers = dict([('Accept', 'application/json')])

        with (http.openClient(bufsize=131072, hostname=host, port=port,
                method=method, path=path, headers=headers, portOptional=True)
                as beta):

            assert not beta.connector.accepted
            assert not beta.connector.connected
            assert not beta.connector.cutoff

            assert beta.requester.portOptional  # portOptional
            assert  beta.requester.lines == []

            if sys.platform != "linux" and sys.platform != "win32":
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
            # port empty inhost header line
            assert  beta.requester.lines == [
                b'GET /echo?name=fame HTTP/1.1',
                b'Host: 127.0.0.1',
                b'Accept-Encoding: identity',
                b'Accept: application/json',
                b'',
                b'']

            assert beta.requester.head == (b'GET /echo?name=fame HTTP/1.1\r\n'
                                           b'Host: 127.0.0.1\r\n'
                                           b'Accept-Encoding: identity'
                                           b'\r\nAccept: application/json\r\n\r\n')

            assert msgOut == (b'GET /echo?name=fame HTTP/1.1\r\n'
                              b'Host: 127.0.0.1\r\n'
                              b'Accept-Encoding: identity'
                              b'\r\nAccept: application/json\r\n\r\n')


            if sys.platform != "linux" and sys.platform != "win32":
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



def test_client_service_echo():
    """
    Test Client service echo nonblocking
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

    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.reopen()
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
        beta.service()
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
        beta.service()
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
    beta.close()


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

    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.reopen()
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
        beta.service()
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
        beta.service()
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
        beta.service()
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
        beta.service()
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
    beta.close()




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

    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.reopen()
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
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.respond()  # returns next response from .responses
    assert response == clienting.Response(version=(1, 1),
                                          status=200,
                                          reason='OK',
                                          headers=Hict([('Content-Length', '122'),
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
                                                   'headers': Hict([('Accept', 'application/json')]),
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
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.close()


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

    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 method=method,
                                 path=path,
                                 headers=headers,
                                 )

    assert beta.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    beta.transmit()

    while (not alpha.ixes or  beta.requests or
           beta.connector.txbs or not beta.respondent.ended):
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended


    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None,
                                    'host': '127.0.0.1',
                                    'port': 6101,
                                    'scheme': 'http'},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.close()


def test_client_pipline_echo_simple_path_scheme():
    """
    Test Client pipeline servicing using path components for host port scheme
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)


    path = "http://{0}:{1}/".format('localhost', alpha.eha[1])  # needed for connect
    # method and headers set in request
    beta = clienting.Client(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.reopen()
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
        mockEchoService(alpha)  # mockEchoServiceLocalhost
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'http',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}
    alpha.close()
    beta.close()


def test_client_pipeline_echo_simple_path_track():
    """
    Test Client pipeline servicing using path components for host port scheme
    Request includes tracking information that is included in reponses copy
    of request
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)


    path = "http://{0}:{1}/".format('localhost', alpha.eha[1])  # needed for connect
    # method and headers set in request
    beta = clienting.Client(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.reopen()
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response ==  {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
    beta.close()


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
    beta = clienting.Client(bufsize=131072,
                                 path=path,
                                 reconnectable=True,  # do not close connection
                                 )

    assert beta.reopen()
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
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json'), ('content-type', 'application/json; charset=utf-8')]),
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
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json'), ('content-type', 'application/json; charset=utf-8')]),
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
    beta.close()


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
    beta = clienting.Client(bufsize=131072,
                             hostname=host,
                             port=port,
                             tymth=tymist.tymen(),  # passed through to connector
                             reconnectable=True,  # passed through to connector
                             tymeout=1.0,  # passed through to connector
                             )
    assert beta.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    while True:
        beta.service()
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
        beta.service()
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
        beta.service()
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
        beta.service()
        if not beta.connector.connected:
            break
        time.sleep(0.1)
        tymist.tick(tock=0.5)

    assert not beta.connector.cutoff

    # auto reconnect
    while True:
        beta.service()
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
        beta.service()
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
        beta.service()
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
    beta.close()



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

    beta = clienting.Client(hostname=serverCertCommonName,
                          port=alpha.eha[1],
                          bufsize=131072,
                          scheme='https',
                          reconnectable=True,
                          certedhost=serverCertCommonName,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,
                        )

    assert beta.reopen()
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'host': 'localhost',
                                    'port': 6101,
                                    'scheme': 'https',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None}

    alpha.close()
    beta.close()


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

    beta = clienting.Client(path=path,
                          bufsize=131072,
                          reconnectable=True,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,
                        )

    assert beta.reopen()
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses)== 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
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
        mockEchoService(alpha)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses)== 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                         'status': 200,
                         'reason': 'OK',
                         'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                     'headers': Hict([('Accept', 'application/json')]),
                                     'body': b'',
                                     'host': 'localhost',
                                     'port': 6101,
                                     'scheme': 'https',
                                     'data': None,
                                     'fargs': None},
                         'errored': False,
                         'error': None}

    alpha.close()
    beta.close()


def test_client_redirect_differnet_path():
    """
    Test Client redirect to different path but same server
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    host = alpha.eha[0]
    port = alpha.eha[1]
    # defaults to redirectable = True
    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 reconnectable=True,
                                 )

    assert beta.reopen()
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
        mockRedirectPathService(alpha)
        time.sleep(0.05)
        beta.service()
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
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
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
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None,
                        'redirects': [{'version': (1, 1),
                                       'status': 307,
                                       'reason': 'Temporary Redirect',
                                       'headers': Hict([('Content-Type', 'text/plain'), ('Content-Length', '0'), ('Access-Control-Allow-Origin', '*'), ('Location', 'http://localhost:6101/redirect?name=fame')]),
                                       'body': bytearray(b''),
                                       'data': None,
                                       'request': {'method': 'GET',
                                                   'path': '/echo',
                                                   'qargs': {'name': 'fame'},
                                                   'fragment': '',
                                                   'headers': Hict([('Accept', 'application/json')]),
                                                   'body': b'',
                                                   'host': '127.0.0.1',
                                                   'port': 6101,
                                                   'scheme': 'http',
                                                   'data': None,
                                                   'fargs': None},
                                       'errored': False,
                                       'error': None}]}

    alpha.close()
    beta.close()



def test_client_redirect_different_servers():
    """
    Test Client redirect from one server to another server
    """
    alpha = tcp.Server(port = 6101, bufsize=131072)
    assert alpha.reopen()
    assert alpha.ha == ('0.0.0.0', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    gamma = tcp.Server(port = 6103, bufsize=131072)
    assert gamma.reopen()
    assert gamma.ha == ('0.0.0.0', 6103)
    assert gamma.eha == ('127.0.0.1', 6103)


    host = alpha.eha[0]
    port = alpha.eha[1]
    # defaults to redirectable = True
    beta = clienting.Client(bufsize=131072,
                                 hostname=host,
                                 port=port,
                                 reconnectable=True,
                                 )

    assert beta.reopen()
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
        mockRedirectingService(alpha)
        mockRedirectedService(gamma)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'host': 'localhost',
                                    'port': 6103,
                                    'scheme': 'http',
                                    'method': 'GET',
                                    'path': '/redirect',
                                    'fragment': '',
                                    'qargs': {'name': 'fame'},
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None,
                        'redirects': [{'version': (1, 1),
                                       'status': 307,
                                       'reason': 'Temporary Redirect',
                                       'headers': Hict([('Content-Type', 'text/plain'), ('Content-Length', '0'), ('Access-Control-Allow-Origin', '*'), ('Location', 'http://localhost:6103/redirect?name=fame')]),
                                       'body': bytearray(b''),
                                       'data': None,
                                       'request': {'method': 'GET',
                                                   'path': '/echo',
                                                   'qargs': {'name': 'fame'},
                                                   'fragment': '',
                                                   'headers': Hict([('Accept', 'application/json')]),
                                                   'body': b'',
                                                   'host': '127.0.0.1',
                                                   'port': 6101,
                                                   'scheme': 'http',
                                                   'data': None,
                                                   'fargs': None},
                                       'errored': False,
                                       'error': None}]}

    alpha.close()
    gamma.close()
    beta.close()



def test_client_redirect_different_servers_tls():
    """
    Test Client redirect to a different server
    """
    serverCertCommonName = 'localhost' # match hostname uses servers's cert commonname
    #serverKeypath = '/etc/pki/tls/certs/server_key.pem'  # local server private key
    #serverCertpath = '/etc/pki/tls/certs/server_cert.pem'  # local server public cert
    #clientCafilepath = '/etc/pki/tls/certs/client.pem' # remote client public cert

    serverKeypath = certdirpath + '/server_key.pem'  # local server private key
    serverCertpath = certdirpath + '/server_cert.pem'  # local server public cert
    clientCafilepath = certdirpath + '/client.pem' # remote client public cert

    alpha = tcp.ServerTls(host=serverCertCommonName,
                               port = 6101,
                               bufsize=131072,
                               context=None,
                               version=None,
                               certify=None,
                               keypath=serverKeypath,
                               certpath=serverCertpath,
                               cafilepath=clientCafilepath,)
    assert alpha.reopen()
    assert alpha.ha == ('127.0.0.1', 6101)
    assert alpha.eha == ('127.0.0.1', 6101)

    gamma = tcp.ServerTls(host=serverCertCommonName,
                               port = 6103,
                               bufsize=131072,
                               context=None,
                               version=None,
                               certify=None,
                               keypath=serverKeypath,
                               certpath=serverCertpath,
                               cafilepath=clientCafilepath)
    assert gamma.reopen()
    assert gamma.ha == ('127.0.0.1', 6103)
    assert gamma.eha == ('127.0.0.1', 6103)

    #clientKeypath = '/etc/pki/tls/certs/client_key.pem'  # local client private key
    #clientCertpath = '/etc/pki/tls/certs/client_cert.pem'  # local client public cert
    #serverCafilepath = '/etc/pki/tls/certs/server.pem' # remote server public cert

    clientKeypath = certdirpath + '/client_key.pem'  # local client private key
    clientCertpath = certdirpath + '/client_cert.pem'  # local client public cert
    serverCafilepath = certdirpath + '/server.pem' # remote server public cert

    host = serverCertCommonName
    port = alpha.eha[1]
    beta = clienting.Client(bufsize=131072,
                          hostname=host,
                          port=port,
                          reconnectable=True,
                          scheme='https',
                          certedhost=serverCertCommonName,
                          keypath=clientKeypath,
                          certpath=clientCertpath,
                          cafilepath=serverCafilepath,)

    assert beta.reopen()
    assert not beta.connector.accepted
    assert not beta.connector.connected
    assert not beta.connector.cutoff

    #  build request
    request = dict([('method', u'GET'),
                     ('path', u'/echo?name=fame'),
                     ('qargs', dict()),
                     ('fragment', u''),
                     ('headers', dict([('Accept', 'application/json')])),
                     ('body', None),
                    ])

    beta.requests.append(request)

    while (beta.requests or beta.connector.txbs or
           not beta.respondent.ended):
        mockRedirectingServiceTls(alpha)
        mockRedirectedService(gamma)
        time.sleep(0.05)
        beta.service()
        time.sleep(0.05)

    assert not beta.connector.rxbs
    assert not beta.waited
    assert beta.respondent.ended

    assert len(beta.responses) == 1
    response = beta.responses.popleft()
    assert response == {'version': (1, 1),
                        'status': 200,
                        'reason': 'OK',
                        'headers': Hict([('Content-Length', '122'), ('Content-Type', 'application/json'), ('Date', 'Thu, 30 Apr 2015 19:37:17 GMT'), ('Server', 'IoBook.local')]),
                        'body': bytearray(b'{"content": null, "query": {"name": "fame"}, "verb": "GE'
                                          b'T", "url": "http://127.0.0.1:8080/echo?name=fame", "acti'
                                          b'on": null}'),
                        'data': {'content': None,
                                 'query': {'name': 'fame'},
                                 'verb': 'GET',
                                 'url': 'http://127.0.0.1:8080/echo?name=fame',
                                 'action': None},
                        'request': {'host': 'localhost',
                                    'port': 6103,
                                    'scheme': 'https',
                                    'method': 'GET',
                                    'path': '/redirect',
                                    'fragment': '',
                                    'qargs': {'name': 'fame'},
                                    'headers': Hict([('Accept', 'application/json')]),
                                    'body': b'',
                                    'data': None,
                                    'fargs': None},
                        'errored': False,
                        'error': None,
                        'redirects': [{'version': (1, 1),
                                       'status': 307,
                                       'reason': 'Temporary Redirect',
                                       'headers': Hict([('Content-Type', 'text/plain'),
                                                          ('Content-Length', '0'),
                                                          ('Access-Control-Allow-Origin', '*'),
                                                          ('Location', 'https://localhost:6103/redirect?name=fame')]),
                                       'body': bytearray(b''),
                                       'data': None,
                                       'request': {'method': 'GET',
                                                   'path': '/echo',
                                                   'qargs': {'name': 'fame'},
                                                   'fragment': '',
                                                   'headers': Hict([('Accept', 'application/json')]),
                                                   'body': b'',
                                                   'host': 'localhost',
                                                   'port': 6101,
                                                   'scheme': 'https',
                                                   'data': None,
                                                   'fargs': None},
                                       'errored': False,
                                       'error': None}]}


    alpha.close()
    gamma.close()
    beta.close()


def test_multipart_form():
    """
    Test multipart form for Requester
    """
    host = u'127.0.0.1'
    port = 6101
    method = u'POST'
    path = u'/echo?name=fame'
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

    part = (b'Content-Disposition: form-data; name="text"\r\n'
            b'Content-Type: text/plain; charset=utf-8\r\n\r\n'
            b'This is the life,\nIt is the best.\n\r\n')
    assert part in msgOut
    part = (b'Content-Disposition: form-data; name="html"\r\n'
            b'Content-Type: text/plain; charset=utf-8\r\n\r\n'
            b'<html><body></body><html>\r\n')
    assert part in msgOut
    assert request.head.startswith((b'POST /echo?name=fame HTTP/1.1\r\n'
                                    b'Host: 127.0.0.1:6101\r\n'
                                    b'Accept-Encoding: identity\r\n'
                                    b'Content-Length: 325\r\n'
                                    b'Accept: application/json\r\n'
                                    b'Content-Type: multipart/form-data; boundary='))


def test_query_quoting():
    """
    Test agorithm for parsing and reassembling query
    """
    location = ('https%3A%2F%2Fapi.twitter.com%2F1.1%2Faccount%2Fverify_credentials.json'
                '?oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU'
                '&oauth_nonce=eb616fe02004000'
                '&oauth_signature_method=HMAC-SHA1'
                '&oauth_timestamp=1437580412'
                '&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD'
                '&oauth_version=1.0'
                '&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')
    path, sep, query = location.partition('?')
    path = unquote(path)
    if sep:
        location = sep.join([path, query])
    else:
        location = path

    assert location == ('https://api.twitter.com/1.1/account/verify_credentials.json'
                        '?oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU'
                        '&oauth_nonce=eb616fe02004000'
                        '&oauth_signature_method=HMAC-SHA1'
                        '&oauth_timestamp=1437580412'
                        '&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD'
                        '&oauth_version=1.0'
                        '&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')

    splits = urlsplit(location)
    query = splits.query
    assert query == ('oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU'
                     '&oauth_nonce=eb616fe02004000'
                     '&oauth_signature_method=HMAC-SHA1'
                     '&oauth_timestamp=1437580412'
                     '&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD'
                     '&oauth_version=1.0'
                     '&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')
    querySplits = query.split('&')
    assert querySplits == ['oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU',
                                    'oauth_nonce=eb616fe02004000',
                                    'oauth_signature_method=HMAC-SHA1',
                                    'oauth_timestamp=1437580412',
                                    'oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD',
                                    'oauth_version=1.0',
                                    'oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D']
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

    assert qargs == {'oauth_consumer_key': u'meWtb1jEOCQciCgqheqiQoU',
                             'oauth_nonce': u'eb616fe02004000',
                             'oauth_signature_method': u'HMAC-SHA1',
                             'oauth_timestamp': u'1437580412',
                             'oauth_token': u'1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD',
                             'oauth_version': u'1.0',
                             'oauth_signature': u'KBD3DdNVZBjyOd0fqQ9X17ack='}
    qargParts = ["{0}={1}".format(key, quote_plus(str(val)))
                 for key, val in qargs.items()]
    newQuery = '&'.join(qargParts)
    assert newQuery == ('oauth_consumer_key=meWtb1jEOCQciCgqheqiQoU'
                        '&oauth_nonce=eb616fe02004000'
                        '&oauth_signature_method=HMAC-SHA1'
                        '&oauth_timestamp=1437580412'
                        '&oauth_token=1048104-WpGhCC4Fbj9Bp5PaTTuN0laSqD4vxCb2B7xh62YD'
                        '&oauth_version=1.0'
                        '&oauth_signature=KBD3DdNVZBjyOd0fqQ9X17ack%3D')


def test_client_port_options():
    beta = clienting.Client(hostname="127.0.0.1",
                            port=4321,
                            scheme='https')
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:4321'
    beta = clienting.Client(hostname="127.0.0.1",
                            port=443,
                            scheme='https')
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:443'
    beta = clienting.Client(hostname="127.0.0.1",
                            port=443,
                            scheme='https',
                            portOptional=True)
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1'

    beta = clienting.Client(hostname="127.0.0.1",
                            port=80,
                            scheme='http')
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:80'
    beta = clienting.Client(hostname="127.0.0.1",
                            port=80,
                            scheme='http',
                            portOptional=True)
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1'

    beta = clienting.Client(hostname="127.0.0.1",
                            port=443,
                            scheme='http',
                            portOptional=True)
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:443'

    beta = clienting.Client(hostname="127.0.0.1",
                            port=5564,
                            scheme='https',
                            portOptional=True)
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:5564'

    beta = clienting.Client(hostname="127.0.0.1",
                            port=1234,
                            scheme='http',
                            portOptional=True)
    beta.requester.build()
    assert beta.requester.lines[1] == b'Host: 127.0.0.1:1234'


if __name__ == '__main__':
    test_client_request_echo()
    test_client_request_echo_port_empty()
    test_client_pipline_echo_simple_tls()
