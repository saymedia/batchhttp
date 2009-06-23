"""

The batch HTTP client provides a convenience interface around an
`httplib2.Http` instance for combining multiple requests into one MIME-encoded
batch request, dispatching the subresponses to the requests' associated
callbacks.

"""

import email
try:
    from email.feedparser import FeedParser
    from email.header import Header
except ImportError:
    from email.Parser import FeedParser
    from email.Header import Header
from httplib import HTTPException
import logging
import mimetools
import new
from StringIO import StringIO
from urlparse import urljoin, urlparse, urlunparse
import weakref

import httplib2

from batchhttp.multipart import MultipartHTTPMessage, HTTPRequestMessage

log = logging.getLogger(__name__)


class BatchError(Exception):
    """An Exception raised when the `BatchClient` cannot open, add, or
    complete a batch request."""
    pass


class NonBatchResponseError(BatchError):
    """An exception raised when the `BatchClient` receives a response 
    with an HTTP status code other than 207."""
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        super(NonBatchResponseError, self).__init__(
            'Received non-batch response: %d %s' % 
            (self.status, self.reason)
        )


class WeaklyBoundMethod(object):

    """A bound method that only weakly holds the instance to which it's bound.

    A `WeaklyBoundMethod` instance is similar to a regular `instancemethod`,
    but if all other references to the method's object are released, the
    method "dies" and can no longer be invoked.

    This implementation is inspired by Peter Parente's article and example
    implementation at http://mindtrove.info/articles/python-weak-references/ .

    """

    def __init__(self, method):
        """Configures this `WeaklyBoundMethod` to be otherwise equivalent to
        the `method` parameter, an `instancemethod`."""
        self.instance  = weakref.ref(method.im_self)
        self.function  = method.im_func
        self.methclass = method.im_class

    def alive(self):
        """Returns whether this `WeaklyBoundMethod` instance still holds its
        referent.

        If all other strong references to the instance to which this method is
        bound are released, the instance will be collected and this method
        will return `False`.

        """
        if self.instance() is None:
            return False
        return True

    def __call__(self, *args, **kwargs):
        """Invokes this `WeaklyBoundMethod` instance.

        If there still exist strong references to the instance to which this
        `WeaklyBoundMethod` is bound, the bound method will be called with all
        the given parameters.

        If there are no more strong references to this method's referent and
        it has been collected, a `ReferenceError` is raised instead.

        You can use its `alive()` method to determine if the
        `WeaklyBoundMethod` instance still has its referent without invoking
        the bound function.

        """
        instance = self.instance()
        if instance is None:
            raise ReferenceError('Instance to which method was weakly bound has been collected')

        method = new.instancemethod(self.function, instance, self.methclass)
        return method(*args, **kwargs)


class WeakCallback(object):

    """A callback that is held through a weak reference.

    Using `WeakCallback` to hold an `instancemethod` will probably not do what
    you mean, as `instancemethod` instances are created on demand when you use
    the instance's attribute of that name. To hold an `instancemethod` while
    weakly referring to its instance, use `WeaklyBoundMethod` instead.

    """

    def __init__(self, callback):
        """Configures this `WeakCallback` instance to weakly refer to callable
        `callback`."""
        self.callback = weakref.ref(callback)

    def alive(self):
        """Returns whether the callable referent of this `WeakCallback`
        instance is still held.

        If all other strong references to the instance to which this method is
        bound are released, the instance will be collected and this method
        will return `False`.

        """
        if self.callback() is None:
            return False
        return True

    def __call__(self, *args, **kwargs):
        """Invokes the referent of this `WeakCallback` instance with the given
        parameters.

        If the target `callable` object of this `WeakCallback` instance no
        longer exists, a `ReferenceError` is raised.

        """
        callback = self.callback()
        if callback is None:
            raise ReferenceError('Callback to which this callback was weakly bound has been collected')
        return callback(*args, **kwargs)


class Request(object):

    """A subrequest of a batched HTTP request.

    A batch request comprises one or more `Request` instances. Once the batch
    request is performed, the subresponses and their contents are dispatched
    to the callbacks of the associated `Request` instances.

    In order to reduce unnecessary subrequests that may be come into being
    before the batch request is completed, `Request` instances hold weak
    references to their callbacks. These weak references are instances of
    either `WeaklyBoundMethod` (if the requested callback is an
    `instancemethod`) or `WeakCallback` (for all other callables).

    If the callback ceases to be referenced by any other code between the
    creation of the `Request` instance and the completion of the batch request
    through a `BatchClient.complete_request()` call, the subrequest will be
    omitted from the batch and the callback will not be called.

    """

    def __init__(self, reqinfo, callback):
        """Initializes the `Request` instance with the given request and
        subresponse callback.

        Parameter `reqinfo` is the HTTP request to perform, specified as a
        mapping of keyword arguments suitable for passing to an
        `httplib2.Http.request()` call.

        Parameter `callback` is the callable object to which to supply the
        subresponse once the batch request is performed. No strong reference
        to `callback` is kept by the `Request` instance, so unless it (or its
        bound instance, if it's an `instancemethod`) continues to be
        referenced elsewhere, the subrequest will be omitted from the batch
        request and `callback` will not be called with a subresponse.

        Callbacks should expect three positional parameters:

        * the URL of the original subrequest
        * an `httplib2.Response` representing the subresponse and its headers
        * the textual body of the subresponse

        """
        self.reqinfo = reqinfo

        if hasattr(callback, 'im_self'):  # instancemethod
            self.callback = WeaklyBoundMethod(callback)
        else:
            self.callback = WeakCallback(callback)

    def alive(self):
        """Returns whether this `Request` instance's callback still exists."""
        return self.callback.alive()

    def _update_headers_from_cache(self, http):
        objreq = self.reqinfo

        if http.cache is not None or http.authorizations:
            class StopCharade(Exception):
                pass

            class CaptureConnections(object):
                def __contains__(self, key):
                    return True
                def __getitem__(self, key):
                    return self.http

            class CaptureHTTPConnection(object):
                def request(self, method, request_uri, body, headers):
                    self.url     = request_uri
                    self.body    = body
                    self.headers = headers
                    raise StopCharade()

            real_connections = http.connections
            conns = CaptureConnections()
            conn = CaptureHTTPConnection()
            conns.http = conn
            http.connections = conns
            try:
                try:
                    http.request(**objreq)
                except StopCharade:
                    return conn.headers, conn.body
            finally:
                # Put the real connections back.
                http.connections = real_connections

        # We didn't finish our request, or there was no cache, so return what
        # we were given.
        return objreq.get('headers', {}), objreq.get('body')

    def _update_response_from_cache(self, http, response, realbody):
        if http.cache is not None or http.authorizations:
            class HandoffConnections(object):
                def __contains__(self, key):
                    return True
                def __getitem__(self, key):
                    return self.http

            class HandoffHTTPConnection(object):
                def request(self, method, request_uri, body, headers):
                    pass

                def read(self):
                    return httplib2._decompressContent(response, realbody)

                def getresponse(self):
                    return self

                def __getattr__(self, key):
                    return getattr(response, key)

            real_connections = http.connections
            fc = HandoffConnections()
            fc.http = HandoffHTTPConnection()
            http.connections = fc
            try:
                response, realbody = http.request(**self.reqinfo)
            finally:
                http.connections = real_connections

            # Fix up the status code, since httplib2 writes the 304 through
            # to the cache, but we want to treat it like a 200.
            if response.status == 304:
                response.status = 200

        return response, realbody

    def as_message(self, http, id):
        """Converts this `Request` instance into a
        `batchhttp.multipart.HTTPRequestMessage` suitable for adding to a
        `batchhttp.multipart.MultipartHTTPMessage` instance.

        If this `Request` instance's callback no longer exists, a
        `ReferenceError` is raised.

        """
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
        """Decodes and dispatches the given subresponse to this `Request`
        instance's callback.

        Parameter `http` is the `httplib2.Http` instance to use for retrieving
        unmodified content from cache, updating with new authorization
        headers, etc. Parameter `part` is the `email.message.Message`
        containing the subresponse content to decode.

        If this `Request` instance's callback no longer exists, a
        `ReferenceError` is raised instead of decoding anything. If the
        subresponse cannot be decoded properly, a `BatchError` is raised.

        """
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

        # httplib2.Response doesn't lower case header keys itself, so a
        # Response from an email Message is inconsistent with one from an
        # httplib.HTTPResponse. Enforce lower case here.
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

    """A collection of HTTP responses that should be performed in a batch as
    one response."""

    def __init__(self):
        self.requests = list()

    def __len__(self):
        """Returns the number of subrequests there are to perform.

        This count *does not include* subrequests that will not be performed
        due to the garbage collection of their callbacks. Callbacks that have
        already expired don't count.

        """
        return len([r for r in self.requests if r.alive()])

    def add(self, reqinfo, callback):
        """Adds a new `Request` instance to this `BatchRequest` instance.

        Parameters `reqinfo` and `callback` should be an HTTP request info
        mapping and a callable object, suitable for using to construct a new
        `Request` instance.

        """
        r = Request(reqinfo, callback)
        self.requests.append(r)

    def process(self, http, endpoint):
        """Performs a batch request.

        Parameter `http` is an `httplib2.Http` instance to use when building
        subrequests and decoding subresponses as well as performing the actual
        batch HTTP request.

        Parameter `endpoint` is a URL specifying where the batch processor is.
        The batch request will be made to the ``/batch-processor`` resource at
        the root of the site named in `endpoint`.

        If this `BatchRequest` instance contains no `Request` instances that
        can deliver their subresponses, no batch request will occur.

        """
        headers, body = self.construct(http)
        if headers and body:
            batch_url = urljoin(endpoint, '/batch-processor')
            response, content = http.request(batch_url, body=body, method="POST", headers=headers)
            self.handle_response(http, response, content)

    def construct(self, http):
        """Builds a batch HTTP request from the `BatchRequest` instance's
        constituent subrequests.

        The batch request is returned as a tuple containing a mapping of HTTP
        headers and the text of the request body.

        """
        if not len(self):
            log.warning('No requests were made for the batch')
            return None, None

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

        # lets prefer gzip encoding on the batch response
        headers['accept-encoding'] = 'gzip;q=1.0, identity; q=0.5, *;q=0'

        return headers, content

    def handle_response(self, http, response, content):
        """Dispatches the subresponses contained in the given batch HTTP
        response to the associated callbacks.

        Parameter `http` is the `httplib2.Http` instance to use for retrieving
        unmodified subresponse bodies, updating authorization headers, etc.
        Parameters `response` and `content` are the `httplib2.Response`
        instance representing the batch HTTP response information and its
        associated text content respectively.

        If the response is not a successful ``207 Multi-Status`` HTTP
        response, or the batch response content cannot be decoded into its
        constituent subresponses, a `BatchError` is raised.

        """
        # was the response okay?
        if response.status != 207:
            log.debug('Received non-batch response %d %s with content:\n%s'
                % (response.status, response.reason, content))
            raise NonBatchResponseError(response.status, response.reason)

        # parse content into pieces

        # Prevent the application/http-response sub-parts from turning into
        # Messages, as the HTTP status line will confuse the parser and
        # we'll just get a text/plain Message with our response for the
        # payload anyway.
        class HttpAverseParser(FeedParser):
            def _parse_headers(self, lines):
                FeedParser._parse_headers(self, lines)
                if self._cur.get_content_type() == 'application/http-response':
                    self._set_headersonly()

        p = HttpAverseParser()
        headers = ""
        for hdr in response:
            headers += "%s: %s\n" % (hdr, Header(response[hdr]).encode(), )

        p.feed(headers)
        p.feed("\n")
        p.feed(content)
        message = p.close()

        if not message.is_multipart():
            log.debug('RESPONSE: ' + str(response))
            log.debug('CONTENT: ' + content)
            raise BatchError('Response was not a MIME multipart response set')

        response = {}
        messages = message.get_payload()

        for part in messages:
            if part.get_content_type() != 'application/http-response':
                raise BatchError('Batch response included a part that was not an HTTP response message')
            try:
                request_id = int(part['Multipart-Request-ID'])
            except KeyError:
                raise BatchError('Batch response included a part with no Multipart-Request-ID header')
            except ValueError:
                raise BatchError('Batch response included a part with an invalid Multipart-Request-ID header')

            request = self.requests[request_id-1]
            try:
                request.decode_response(http, part)
            except ReferenceError:
                # We shouldn't have lost any references to request objects
                # since the request, but just in case.
                pass


class BatchClient(httplib2.Http):

    """Sort of an HTTP client for performing a batch HTTP request."""

    def __init__(self, endpoint=None):
        """Configures the `BatchClient` instance to use the given batch
        processor endpoint.

        Parameter `endpoint` is the base URL at which to find the batch
        processor to which to submit the batch request. The batch processor
        should be the resource ``/batch-processor`` at the root of the site
        specified in `endpoint`.

        """
        self.endpoint = endpoint
        super(BatchClient, self).__init__()

    def batch_request(self):
        """Opens a batch request.

        If a batch request is already open, a `BatchError` is raised.

        In Python 2.5 or later, you can use this method with the ``with``
        statement::

        >>> with client.batch_request():
        ...     client.batch({'uri': uri}, callback=handle_result)

        The batch request is then completed automatically at the end of the
        ``with`` block.

        """
        import traceback
        if hasattr(self, 'batchrequest'):
            # hey, we already have a request. this is invalid...
            log.debug('Batch request previously opened at:\n'
                + ''.join(traceback.format_list(self._opened)))
            log.debug('New now at:\n' + ''.join(traceback.format_stack()))
            raise BatchError("There's already an open batch request")
        self.batchrequest = BatchRequest()
        self._opened = traceback.extract_stack()

        # Return ourself so we can enter a "with" context.
        return self

    def complete_batch(self):
        """Closes a batch request, submitting it and dispatching the
        subresponses.

        If no batch request is open, a `BatchError` is raised.

        """
        if not hasattr(self, 'batchrequest'):
            raise BatchError("There's no open batch request to complete")
        if self.endpoint is None:
            raise BatchError("There's no batch processor endpoint to which to send a batch request")
        try:
            log.warning('Making batch request for %d items' % len(self.batchrequest))
            self.batchrequest.process(self, self.endpoint)
        finally:
            del self.batchrequest

    def clear_batch(self):
        """Closes a batch request without performing it."""
        try:
            del self.batchrequest
        except AttributeError:
            # well it's already cleared then isn't it
            pass

    def batch(self, reqinfo, callback):
        """Adds the given subrequest to the batch request.

        Parameter `reqinfo` is the HTTP request to perform, specified as a
        mapping of keyword arguments suitable for passing to an
        `httplib2.Http.request()` call.

        Parameter `callback` is the callable object to which to supply the
        subresponse once the batch request is performed. No strong reference
        to `callback` is kept by the `Request` instance, so unless it (or its
        bound instance, if it's an `instancemethod`) continues to be
        referenced elsewhere, the subrequest will be omitted from the batch
        request and `callback` will not be called with a subresponse.

        If no batch request is open, a `BatchError` is raised.

        """
        if not hasattr(self, 'batchrequest'):
            raise BatchError("There's no open batch request to add an object to")
        self.batchrequest.add(reqinfo, callback)

    def request(self, uri, method="GET", body=None, headers=None, redirections=httplib2.DEFAULT_MAX_REDIRECTS, connection_type=None):
        req_log = logging.getLogger('.'.join((__name__, 'request')))
        if req_log.isEnabledFor(logging.DEBUG):
            if headers is None:
                headeritems = ()
            else:
                headeritems = headers.items()
            req_log.debug('Making request:\n%s %s\n%s\n\n%s', method, uri,
                '\n'.join([
                    '%s: %s' % (k, v) for k, v in headeritems
                ]), body or '')

        response, content = super(BatchClient, self).request(uri, method, body, headers, redirections, connection_type)

        resp_log = logging.getLogger('.'.join((__name__, 'response')))
        if resp_log.isEnabledFor(logging.DEBUG):
            resp_log.debug('Got response:\n%s\n\n%s',
                '\n'.join([
                    '%s: %s' % (k, v) for k, v in response.items()
                ]), content)

        return response, content

    def __enter__(self):
        return self.batchrequest

    def __exit__(self, *exc_info):
        if None not in exc_info:
            # Exception! Let's forget the whole thing.
            self.clear_batch()
        else:
            # Finished the context. Try to complete the request.
            self.complete_batch()
