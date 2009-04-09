import email
import email.feedparser
import email.header
from httplib import HTTPException
import httplib2
import logging
import mimetools
import new
from StringIO import StringIO
from urlparse import urljoin, urlparse, urlunparse
import weakref

from batchhttp.multipart import MultipartHTTPMessage, HTTPRequestMessage

__all__ = ('BatchClient', 'client', 'log')

log = logging.getLogger('batchhttp.client')

# FIXME: shouldn't be necessary... endpoint URL should
# be able to handle batch requests.
BATCH_ENDPOINT = 'http://127.0.0.1:8000/'

class BatchError(Exception):
    pass

class WeaklyBoundMethod(object):
    # Inspired by http://mindtrove.info/articles/python-weak-references/

    def __init__(self, method):
        self.instance  = weakref.ref(method.im_self)
        self.function  = method.im_func
        self.methclass = method.im_class

    def alive(self):
        if self.instance() is None:
            return False
        return True

    def __call__(self, *args, **kwargs):
        instance = self.instance()
        if instance is None:
            raise ReferenceError('Instance to which method was weakly bound has been collected')

        method = new.instancemethod(self.function, instance, self.methclass)
        return method(*args, **kwargs)

class WeakCallback(object):
    def __init__(self, callback):
        self.callback = weakref.ref(callback)

    def alive(self):
        if self.callback() is None:
            return False
        return True

    def __call__(self, *args, **kwargs):
        return self.callback()(*args, **kwargs)

class Request(object):
    def __init__(self, reqinfo, callback):
        self.reqinfo = reqinfo

        if hasattr(callback, 'im_self'):  # instancemethod
            self.callback = WeaklyBoundMethod(callback)
        else:
            self.callback = WeakCallback(callback)

    def _update_headers_from_cache(self, http):
        objreq = self.reqinfo

        if http.cache is not None or http.authorizations:
            class StopCharade(Exception):
                pass

            # TODO: implement this with a fake "connection" instead of
            # overriding Http methods
            class VolatileHttp(httplib2.Http):
                def _conn_request(self, conn, request_uri, method, body, headers):
                    self.url     = request_uri
                    self.body    = body
                    self.headers = headers
                    raise StopCharade()

            vh = VolatileHttp()
            vh.cache          = http.cache
            vh.authorizations = http.authorizations

            try:
                vh.request(**objreq)
            except StopCharade:
                return vh.headers, vh.body

        # We didn't finish our _request, or there was no cache, so return what
        # we were given.
        return objreq.get('headers', {}), objreq.get('body')

    def _update_response_from_cache(self, http, response, realbody):
        if http.cache is not None or http.authorizations:
            # TODO: implement this with a fake "connection" instead of
            # overriding Http methods
            class FauxHttp(httplib2.Http):
                def _conn_request(self, conn, request_uri, method, body, headers):
                    return response, httplib2._decompressContent(response, realbody)

            fh = FauxHttp()
            fh.cache            = http.cache
            fh.authorizations   = http.authorizations
            fh.follow_redirects = False

            # Let Http.request fill in the response from its cache.
            response, realbody = fh.request(**self.reqinfo)

            # TODO: Fix up the status code, since httplib2 writes it through
            # to the cache, who knows why.
            if response.status == 304:
                response.status = 200

        return response, realbody

    def as_message(self, http, id):
        if not self.callback.alive():
            raise ReferenceError("No callback to return request's response to")

        headers, body = self._update_headers_from_cache(http)

        objreq = self.reqinfo
        url = objreq['uri']
        parts = urlparse(url)
        host = parts[1]

        # Use whole URL in request line per HTTP/1.1 5.1.2 (proxy behavior).
        requesttext = "GET %s HTTP/1.1\r\n" % url
        headers['host'] = host
        # Prevent compression as it's unlikely to survive batching.
        headers['accept-encoding'] = 'identity'
        for header, value in headers.iteritems():
            requesttext += "%s: %s\r\n" % (header, value)
        requesttext += '\r\n'
        requesttext += body or ''

        requesttext = requesttext.encode('ascii')
        submsg = HTTPRequestMessage(requesttext, id)
        return submsg

    def decode_response(self, http, part):
        if not self.callback.alive():
            raise ReferenceError("No callback to return response to")

        # Parse the part body into a status line and a Message.
        messagetext = part.get_payload(decode=True)
        messagefile = StringIO(messagetext)
        status_line = messagefile.readline()
        message = email.message_from_file(messagefile)

        if status_line.startswith('HTTP/'):
            status_code = status_line.split(' ')[1]
        else:
            status_code = status_line.split(' ')[0]
        message['status'] = int(status_code)

        httpresponse = httplib2.Response(message)
        # TODO: httplib2.Response doesn't lower case header keys itself,
        # so a Response from an email Message is inconsistent with one from
        # an httplib.HTTPResponse. Enforce lower case ourselves for now.
        for k, v in httpresponse.items():
            del httpresponse[k]
            httpresponse[k.lower()] = v

        body = message.get_payload()
        if body is None:
            raise BatchError('Could not decode subrequest body from MIME payload')
        httpresponse, body = self._update_response_from_cache(http, httpresponse, body)
        if body is None:
            raise BatchError('Could not decode subrequest body through httplib2')

        self.callback(self.reqinfo['uri'], httpresponse, body)

class BatchRequest(object):
    def __init__(self):
        self.requests = list()

    def add(self, reqinfo, callback):
        r = Request(reqinfo, callback)
        self.requests.append(r)

    def process(self, http, endpoint):
        headers, body = self.construct(http)
        batch_url = urljoin(endpoint, '/batch-processor')
        response, content = http.request(batch_url, body=body, method="POST", headers=headers)
        self.handle_response(http, response, content)

    def construct(self, http):
        msg = MultipartHTTPMessage()
        request_id = 1
        for request in self.requests:
            try:
                submsg = request.as_message(http, request_id)
            except ReferenceError:
                pass
            else:
                msg.attach(submsg)
            request_id += 1

        # Do this ahead of getting headers, since the boundary is not
        # assigned until we bake the multipart message:
        content = msg.as_string(write_headers=False)
        hdrs = msg.items()
        headers = {}
        for hdr in hdrs:
            headers[hdr[0]] = hdr[1]

        log.debug('Built batch request:\n%s\n\n%s'
            % ('\n'.join([
                '%s: %s' % (k, v) for k, v in headers.items()
            ]), content))

        return headers, content

    def handle_response(self, http, response, content):
        # was the response okay?
        if response.status != 207:
            log.debug('Received non-batch response %d %s with content:\n%s'
                % (response.status, response.reason, content))
            raise BatchError('Received non-batch response: %d %s'
                % (response.status, response.reason))

        # parse content into pieces

        # Prevent the message/http-response sub-parts from turning into
        # Messages, as the HTTP status line will confuse the parser and
        # we'll just get a text/plain Message with our response for the
        # payload anyway.
        class HttpAverseParser(email.feedparser.FeedParser):
            def _parse_headers(self, lines):
                email.feedparser.FeedParser._parse_headers(self, lines)
                if self._cur.get_content_type() == 'message/http-response':
                    self._set_headersonly()

        p = HttpAverseParser()
        headers = ""
        for hdr in response:
            headers += "%s: %s\n" % (hdr, email.header.Header(response[hdr]).encode(), )

        p.feed(headers)
        p.feed("\n")
        p.feed(content)
        message = p.close()

        if not message.is_multipart():
            log.debug('RESPONSE: ' + str(response))
            log.debug('CONTENT: ' + content)
            raise HTTPException('Response was not a MIME multipart response set')

        response = {}
        messages = message.get_payload()

        for part in messages:
            if part.get_content_type() != 'message/http-response':
                raise HTTPException('Batch response included a part that was not an HTTP response message')
            try:
                request_id = int(part['Multipart-Request-ID'])
            except KeyError:
                raise HTTPException('Batch response included a part with no Multipart-Request-ID header')
            except ValueError:
                raise HTTPException('Batch response included a part with an invalid Multipart-Request-ID header')

            request = self.requests[request_id-1]
            try:
                request.decode_response(http, part)
            except ReferenceError:
                # We shouldn't have lost any references to request objects
                # since the request, but just in case.
                pass

class BatchClient(object):

    def __init__(self, http=None, endpoint=None):
        if http is None:
            http = httplib2.Http()
        self.http = http

        if endpoint is None:
            endpoint = BATCH_ENDPOINT
        self.endpoint = endpoint
        # TODO: set up caching?

    def batch_request(self):
        """Opens a new BatchRequest.

        If a request is already instantiated, this will raise an exception.

        """
        import traceback
        if hasattr(self, 'request'):
            # hey, we already have a request. this is invalid...
            log.debug('Batch request previously opened at:\n'
                + ''.join(traceback.format_list(self._opened)))
            log.debug('New now at:\n' + ''.join(traceback.format_stack()))
            raise BatchError("There's already an open batch request")
        self.request = BatchRequest()
        self._opened = traceback.extract_stack()
        return self.request

    def complete_request(self):
        if not hasattr(self, 'request'):
            raise BatchError("There's no open batch request to complete")
        try:
            self.request.process(self.http, self.endpoint)
        finally:
            del self.request

    def clear_request(self):
        try:
            del self.request
        except AttributeError:
            # well it's already cleared then isn't it
            pass

    def add(self, reqinfo, callback):
        if not hasattr(self, 'request'):
            raise BatchError("There's no open batch request to add an object to")
        self.request.add(reqinfo, callback)
