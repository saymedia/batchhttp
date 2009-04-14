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
    """An Exception raised when the `BatchClient` cannot open, add, or
    complete a batch request."""
    pass


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

    """A collection of HTTP responses that should be performed in a batch as
    one response."""

    def __init__(self):
        self.requests = list()

    def __len__(self):
        """Returns the number of subrequests there are.

        This count *includes* subrequests that will not be performed due to
        the garbage collection of their callbacks.

        """
        return len(self.requests)

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

        log.debug('Built batch request:\n%s\n\n%s'
            % ('\n'.join([
                '%s: %s' % (k, v) for k, v in headers.items()
            ]), content))

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
            raise BatchError('Response was not a MIME multipart response set')

        response = {}
        messages = message.get_payload()

        for part in messages:
            if part.get_content_type() != 'message/http-response':
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


class BatchClient(object):

    """Sort of an HTTP client for performing a batch HTTP request."""

    def __init__(self, http=None, endpoint=None):
        """Configures the `BatchClient` instance to use the given user agent
        object and batch processor endpoint.

        Optional parameter `http` specifies an `httplib2.Http` instance to use
        for making the batch request and simulating its subrequests. If not
        given, a new `httplib2.Http` instance is used.

        Optional parameter `endpoint` is the base URL at which to find the
        batch processor to which to submit the batch request. The batch
        processor should be the resource ``/batch-processor`` at the root of
        the site specified in `endpoint`.

        """
        if http is None:
            http = httplib2.Http()
        self.http = http

        if endpoint is None:
            endpoint = BATCH_ENDPOINT
        self.endpoint = endpoint
        # TODO: set up caching?

    def batch_request(self):
        """Opens a batch request.

        If a batch request is already open, a `BatchError` is raised.

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
        """Closes a batch request, submitting it and dispatching the
        subresponses.

        If no batch request is open, a `BatchError` is raised.

        """
        if not hasattr(self, 'request'):
            raise BatchError("There's no open batch request to complete")
        try:
            log.warning('Making batch request for %d items' % len(self.request))
            self.request.process(self.http, self.endpoint)
        finally:
            del self.request

    def clear_request(self):
        """Closes a batch request without performing it."""
        try:
            del self.request
        except AttributeError:
            # well it's already cleared then isn't it
            pass

    def add(self, reqinfo, callback):
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
        if not hasattr(self, 'request'):
            raise BatchError("There's no open batch request to add an object to")
        self.request.add(reqinfo, callback)
