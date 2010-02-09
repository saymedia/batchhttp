# Copyright (c) 2010 Six Apart Ltd.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of Six Apart Ltd. nor the names of its contributors may
#   be used to endorse or promote products derived from this software without
#   specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from twisted.internet import reactor, defer
from twisted.web import proxy, server, http
from twisted.internet import interfaces
from twisted.python import log
from zope.interface import implements
from urllib import quote
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from quopri import decodestring as qdecode
from email.MIMEMessage import MIMEMessage
from email.Message import Message
import urlparse
import base64
import sys
from batchhttp import multipart
log.startLogging(sys.stdout)

from twisted.internet.protocol import Factory
Factory.noisy = False # stfu.

CRLF = "\r\n"


class StringTransport(http.StringTransport):
    def loseConnection(self):
        # http.StringTransport doesn't implement this for some reason.
        pass


class BatchProxyClient(proxy.ProxyClient):
    def __init__(self, deferred, command, rest, version, headers, data, father):
        self.father = father
        self.command = command
        self.rest = rest
        headers.append(('connection', 'close'))
        self.headers = headers
        self.data = data
        self.deferred = deferred

    def connectionMade(self):
        self.sendCommand(self.command, self.rest)
        for header, value in self.headers:
            self.sendHeader(header, value)
        self.endHeaders()
        self.transport.write(self.data)

    def handleResponseEnd(self):
        self.transport.loseConnection()
        self.deferred.callback("response generated")


class BatchProxyClientFactory(proxy.ProxyClientFactory):
    protocol = BatchProxyClient

    def __init__(self, *args, **kwargs):
        proxy.ProxyClientFactory.__init__(self, *args, **kwargs)
        self.deferred = defer.Deferred()

    def buildProtocol(self, addr):
        return self.protocol(self.deferred, self.command, self.rest, self.version,
                             self.headers, self.data, self.father)

    def clientConnectionFailed(self, connector, reason):
        proxy.ProxyClientFactory.clientConnectionFailed(self, connector, reason)
        self.deferred.errback(reason)


class BatchRequest(object):
    def __init__(self, host, port, request, reactor=reactor):
        self.host = host
        self.port = port
        self.request = request
        self.reactor = reactor
        self.transport = StringTransport()

    def process(self):
        """
        Render a request by forwarding it to the proxied server.
        """
        client_factory = BatchProxyClientFactory(self.request.command, self.request.path, 
                                                 self.request.version, self.request.headers, 
                                                 self.request.data, self)
        self.reactor.connectTCP(self.host, self.port, client_factory)
        return client_factory.deferred


class BatchProxyResource(proxy.ReverseProxyResource):
    response_code = http.MULTI_STATUS
    server = 'BatchProxy/0.1'

    def __init__(self, host, port, batch_path):
        proxy.ReverseProxyResource.__init__(self, host, port, '')
        self.batch_path = batch_path

    def getChild(self, path, request):
        # Set x-forwarded-host before the request is sent to the application server.
        request.received_headers['x-forwarded-host'] = request.received_headers['host']

        if path == self.batch_path:
            return self
        else:
            return proxy.ReverseProxyResource(self.host, self.port, '/' + quote(path, safe=""))

    def parse_batch_request(self, request):
        message = StringIO()
        message.write("Content-type: %s%s" % (request.received_headers['content-type'], CRLF))
        message.write("Mime-version: %s%s" % (request.received_headers.get('mime-version', 1.0), CRLF))
        request.content.seek(0, 0)
        message.write(request.content.read())
        message.seek(0, 0)
        parser = multipart.HTTPParser(message)

        requests = parser.requests
        for request in requests:
            request.headers = [header for header in request.headers if header[0].lower() not in ('connection', 'proxy-connection')]
        return requests

    def render_batch(self, results, requests, client):
        message = multipart.MultipartHTTPMessage()
        for batch_request, response in zip(requests, results):
            batch_request.transport.seek(0,0)
            message.attach(multipart.HTTPResponseMessage(batch_request.transport.getvalue(), batch_request.request.request_id))
        message_string = message.as_string(write_headers=False)
        headers = CRLF.join((
            "%s %s %s" % (http.protocol_version, 
                          self.response_code, 
                          http.responses[self.response_code]),
            "Date: %s" % http.datetimeToString(),
            "Server: %s" % self.server,
            "Allow: POST",
            "Content-length: %s" % len(message_string),
            "Content-type: %s" % message.get('content-type'),
            "Mime-version: %s" % message.get('mime-version', 1.0),
        ))
        client.transport.write(headers)
        client.transport.write(CRLF)
        client.transport.write(CRLF)
        client.transport.write(message_string)
        client.channel.transport.loseConnection()

    def render(self, request):
        if request.method.lower() != 'post':
            from twisted.web.server import UnsupportedMethod
            raise UnsupportedMethod(('POST',))

        batch_requests = [BatchRequest(self.host, self.port, r) for r in self.parse_batch_request(request)]
        deferreds = [r.process() for r in batch_requests]
        defer.DeferredList(deferreds, consumeErrors=True).addCallback(self.render_batch, batch_requests, request)
        return server.NOT_DONE_YET


if __name__ == '__main__':
    # python batchproxy.py host:port remote_host:remote_port
    import sys
    iface = ''
    port = 8080
    remote_host = 'localhost'
    remote_port = 8000
    if len(sys.argv) > 1:
        iface_port = sys.argv[1].split(':')
        iface = iface_port[0]
        if len(iface_port) == 2:
            port = int(iface_port[1])
        else:
            port = 8080
    if len(sys.argv) > 2:
        host_port = sys.argv[2].split(':')
        remote_host = host_port[0]
        if len(host_port) == 2:
            remote_port = int(host_port[1])
        else:
            remote_port = 8000

    site = server.Site(BatchProxyResource(remote_host, remote_port, 'batch-processor'))
    reactor.listenTCP(port, site, interface=iface)
    reactor.run()
