# Copyright (c) 2009-2010 Six Apart Ltd.
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

from email.Message import Message
from email.Generator import Generator
from email.MIMEText import MIMEText
from email.MIMEMessage import MIMEMessage
from email.Parser import Parser
from urlparse import urlparse, urlunparse
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import base64
import quopri


def bdecode(s):
    # Can't use base64.decodestring() because it tacks on a newline.
    if not s:
        return s
    value = base64.decodestring(s)
    if not s.endswith("\n") and value.endswith("\n"):
        return value[:-1]
    return value


def parse_uri(uri):
    """Parse a URI. Return the scheme, the host, and the rest of the URI."""
    parts = list(urlparse(uri))
    return parts[0], parts[1], urlunparse([None, None] + parts[2:])


class BadRequestException(Exception): pass
class BadResponseException(Exception): pass
class ParserError(Exception): pass


class HTTPRequest(object):
    def __init__(self, request, headers=None, request_id=None):
        self.length = None
        self.content_type = None
        if not headers:
            headers = []
        self.headers = headers
        self.request_id = request_id

        lines = request.split("\r\n")
        request_line = lines.pop(0)
        parts = request_line.split()
        try:
            self.command = parts[0]
            self.request_uri = parts[1]
            self.version = parts[2]
        except IndexError:
            raise BadRequestException()
        self.scheme, self.host, self.path = parse_uri(self.request_uri)

        line = lines.pop(0)
        # IE sends an extraneous empty line (\r\n) after a POST request;
        # ignore such a line, but only once.
        if not line and self.command == 'POST':
            line = lines.pop(0)
        header = ''
        while line.strip():
            if line[0] in ' \t':
                header = '%s\n%s' % (header, line)
            else:
                if header:
                    self.process_header(header)
                header = line
            if len(lines):
                line = lines.pop(0)
            else:
                break
        if header:
            self.process_header(header)

        # the rest is response body
        self.data = "\r\n".join(lines)

    def process_header(self, line):
        header, data = line.split(':', 1)
        header = header.lower()
        data = data.strip()
        if header == 'content-length':
            self.length = int(data)
        elif header == 'content-type':
            self.content_type = data
        elif header == 'host' and not self.host:
            self.host = data
        self.headers.append((header, data))

    def __str__(self):
        command = "%s %s %s" % (self.command, self.path, self.version)
        headers = "\r\n".join(["%s: %s" % (header, value) for header, value in self.headers])
        return "\r\n".join((command, headers, self.data))


class HTTPResponse(object):
    def __init__(self, response):
        self.length = None
        self.content_type = None

        lines = response.split("\r\n")
        response_line = lines.pop(0)
        parts = response_line.split()
        try:
            self.version = parts[0]
            self.status = parts[1]
        except IndexError:
            raise BadResponseException()
        try:
            self.message = parts[2]
        except IndexError:
            self.message = '' # sometimes there is no message

        self.headers = []
        line = lines.pop(0)
        while line.strip():
            header, value = line.split(':', 1)
            header = header.lower()
            value = value.lstrip()
            self.headers.append((header, value))
            if header == 'content-length':
                self.length = int(value)
            elif header == 'content-type':
                self.content_type = value
            line = lines.pop(0)

        # the rest is response body
        self.data = "\r\n".join(lines)

    def __str__(self):
        status = "%s %s %s" % (self.version, self.status, self.message)
        headers = "\r\n".join(("%s: %s" % (header, value) for header, value in self.headers))
        return "\r\n".join((status, headers, self.data))


class HTTPParser(object):
    def __init__(self, message):
        self._parser = Parser()
        self.requests = []
        self.responses = []
        if isinstance(message, basestring):
            self._parsestr(message)
        else:
            self._parse(message)

    def _parse_subrequest(self, subrequest):
        payload = subrequest.get_payload()
        if payload is None:
            raise ParserError("Missing payload in subrequest")

        content_type = subrequest.get('content-transfer-encoding', '').lower()
        if content_type == 'quoted-printable':
            payload = quopri.decodestring(payload)
        elif content_type == 'base64':
            payload = bdecode(payload)
        return payload

    def _parse(self, fp):
        msg = self._parser.parse(fp)
        for subrequest in msg.walk():
            type = subrequest.get_content_maintype()
            request_id = subrequest.get('multipart-request-id', None)
            if type == 'multipart':
                continue # walk will descend into child messages
            if type == 'application':
                payload = self._parse_subrequest(subrequest)
                subtype = subrequest.get_content_subtype()
                if subtype == 'http-request':
                    self.requests.append(HTTPRequest(payload, request_id=request_id))
                elif subtype == 'http-response':
                    self.responses.append(HTTPResponse(payload))
                else:
                    raise ParserError("Unrecognized message type: '%s'" % subrequest.get_content_type())

    def _parsestr(self, text):
        self._parse(StringIO(text))


class HTTPGenerator(Generator):
    def __init__(self, outfp, mangle_from_=True, maxheaderlen=78, write_headers=True):
        self.write_headers = write_headers
        Generator.__init__(self, outfp, mangle_from_, maxheaderlen)

    def _process_application_http(self, msg):
        payload = msg.get_payload()
        if payload is None:
            return
        if not isinstance(payload, basestring):
            raise TypeError('string payload expected: %s' % type(payload))
        self._fp.write(payload)

    def _handle_application_http_request(self, msg):
        # Called by Generator to parse MIME messages with a
        # content-type of application/http-request.
        self._process_application_http(msg)

    def _handle_application_http_response(self, msg):
        # Called by Generator to parse MIME messages with a
        # content-type of application/http-response.
        self._process_application_http(msg)

    def _write_headers(self, msg):
        if self.write_headers:
            Generator._write_headers(self, msg)


class HTTPMessage(Message):
    def as_string(self, unixfrom=False, write_headers=True):
        fp = StringIO()
        g = HTTPGenerator(fp, write_headers=write_headers)
        g.flatten(self, unixfrom=unixfrom)
        return fp.getvalue()


class MultipartHTTPMessage(HTTPMessage):
    def __init__(self):
        HTTPMessage.__init__(self)
        self.set_type('multipart/parallel')
        self.preamble = "HTTP MIME Message\n"


class HTTPRequestMessage(HTTPMessage):
    def __init__(self, http_request, request_id):
        HTTPMessage.__init__(self)
        self.set_type('application/http-request')
        self.add_header('Multipart-Request-ID', str(request_id))
        self.add_header('Content-transfer-encoding', 'quoted-printable')
        payload = StringIO()
        quopri.encode(StringIO(http_request), payload, quotetabs=False)
        self.set_payload(payload.getvalue())
        payload.close()


class HTTPResponseMessage(HTTPMessage):
    def __init__(self, http_response, request_id):
        HTTPMessage.__init__(self)
        self.set_type('application/http-response')
        self.add_header('Multipart-Request-ID', str(request_id))
        self.add_header('Content-transfer-encoding', 'quoted-printable')
        payload = StringIO()
        quopri.encode(StringIO(http_response), payload, quotetabs=False)
        self.set_payload(payload.getvalue())
        payload.close()

if __name__ == '__main__':
    requests = [
        "GET /users/1.json HTTP/1.1\r\nUser-Agent: curl/7.16.3 (powerpc-apple-darwin9.0) libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3\r\nHost: 127.0.0.1:5001\r\nAccept: */*\r\n\r\n",
        "GET /groups/1.json HTTP/1.1\r\nUser-Agent: curl/7.16.3 (powerpc-apple-darwin9.0) libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3\r\nHost: 127.0.0.1:5001\r\nAccept: */*\r\n\r\n",
        "GET /users/@self HTTP/1.1\r\nUser-Agent: curl/7.16.3 (powerpc-apple-darwin9.0) libcurl/7.16.3 OpenSSL/0.9.7l zlib/1.2.3\r\nHost: 127.0.0.1:5001\r\nAccept: */*\r\n\r\n",
    ]

    msg = MultipartHTTPMessage()
    for id, request in enumerate(requests):
        msg.attach(HTTPRequestMessage(request, id))
    batch_request = msg.as_string()
    print batch_request

    responses = [
        'HTTP/1.0 200 OK\r\nDate: Sat, 28 Feb 2009 01:06:38 GMT\r\nServer: WSGIServer/0.1 Python/2.5.1\r\nVary: Accept-Encoding\r\nETag: "c09c60cdd450593b54e17544fefbdd25"\r\ncontent-type: application/json\r\nAllow: OPTIONS, GET, HEAD\r\n\r\n{\n  "interests": [], \n  "preferredUsername": "dhough", \n  "displayName": "Deidra Hough", \n  "aboutMe": "Velit ridiculus massa a aenean.", \n  "email": "dhough@dreateorposu.com", \n  "accounts": [], \n  "urls": [], \n  "userpic": "", \n  "id": "tag:typepad.com,2003:user-1", \n  "objectType": "tag:api.typepad.com,2009:User"\n}',
        'HTTP/1.0 200 OK\r\nDate: Sat, 28 Feb 2009 01:09:04 GMT\r\nServer: WSGIServer/0.1 Python/2.5.1\r\nLast-Modified: 2009-02-25 21:49:54.824405\r\nETag: "tag:typepad.com,2003:group-1:1235598594.0"\r\ncontent-type: application/json\r\nAllow: OPTIONS, GET, HEAD\r\nVary: Accept-Encoding\r\n\r\n{\n  "displayName": "Risus Urna Ve", \n  "members": "/groups/1/memberships.json", \n  "links": [\n    "http://tesakedre.com/feate/areake/fooronite"\n  ], \n  "tagline": "Vestibulum molestie egestas elit mi lorem at curae porttitor condimentum dis et.", \n  "avatar": "http://www.gein.com/itewestere/drecurliate/akeme", \n  "urls": [], \n  "id": "tag:typepad.com,2003:group-1", \n  "objectType": "tag:api.typepad.com,2009:Group"\n}',
        'HTTP/1.0 401 UNAUTHORIZED\r\nDate: Sat, 28 Feb 2009 01:10:44 GMT\r\nServer: WSGIServer/0.1 Python/2.5.1\r\nVary: Cookie\r\nContent-Type: text/html; charset=utf-8\r\nWWW-Authenticate: Basic realm="Astropad"\r\nWWW-Authenticate: OAuth realm="Astropad"\r\n\r\n',
    ]
    msg = MultipartHTTPMessage()
    for id, response in enumerate(responses):
        msg.attach(HTTPResponseMessage(response, id))
    batch_response = msg.as_string()
    print batch_response

    p = HTTPParser(batch_request)
    for request in p.requests:
        print request

    p = HTTPParser(batch_response)
    for response in p.responses:
        print response
