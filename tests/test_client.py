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

import email
try:
    from email import message
except ImportError:
    import email.Message as message
import httplib
import logging
import re
import unittest

import httplib2
import mox
import nose

import batchhttp.client
from batchhttp.client import BatchClient, BatchError, NonBatchResponseError
from tests import utils


class TestBatchRequests(unittest.TestCase):

    def mocksetter(self, key):
        def mockset(x):
            setattr(self, key, x)
            return True
        return mox.Func(mockset)

    def test_least(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="=={{[[ ASFDASF ]]}}=="',
        })
        content  = """OMG HAI

--=={{[[ ASFDASF ]]}}==
Content-Type: application/http-response
Multipart-Request-ID: 1

200 OK
Content-Type: application/json

{"name": "Potatoshop"}
--=={{[[ ASFDASF ]]}}==--"""

        self.body, self.headers = None, None

        bat = BatchClient(endpoint='http://127.0.0.1:8000/batch-processor')

        m = mox.Mox()
        m.StubOutWithMock(bat, 'request')
        bat.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        bat.cache = None
        bat.authorizations = []

        m.ReplayAll()

        def callback(url, subresponse, subcontent):
            self.subresponse = subresponse
            self.subcontent  = subcontent

        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callback=callback)
        bat.complete_batch()

        m.VerifyAll()

        self.assert_(self.headers is not None)
        headers = sorted([h.lower() for h in self.headers.keys()])
        self.assertEquals(headers, ['accept-encoding', 'content-type', 'mime-version'])
        self.assertEquals(self.headers['MIME-Version'], '1.0')

        # Parse the headers through email.message to test the Content-Type value.
        mess = message.Message()
        for header, value in self.headers.iteritems():
            mess[header] = value
        self.assertEquals(mess.get_content_type(), 'multipart/parallel')
        boundary = mess.get_param('boundary')
        self.assert_(boundary)

        # Check that the multipart request we sent was composed correctly.
        preamble, subresponse, postamble = self.body.split('--%s' % (boundary,))
        self.assert_(None not in (preamble, subresponse, postamble))
        # Trim leading \n left over from the boundary.
        self.assert_(subresponse.startswith('\n'))
        subresponse = subresponse[1:]
        subresp_msg = email.message_from_string(subresponse)
        self.assertEquals(subresp_msg.get_content_type(), 'application/http-request')
        self.assert_('Multipart-Request-ID' in subresp_msg)

        self.assertEquals(self.subcontent, '{"name": "Potatoshop"}')

    def test_bad_response(self):

        response = httplib2.Response({
            'status': '500',
            'content-type': 'text/plain',
        })
        content  = """ o/` an error occurred o/` """

        self.body, self.headers = None, None

        bat = BatchClient(endpoint='http://127.0.0.1:8000/batch-processor')

        m = mox.Mox()
        m.StubOutWithMock(bat, 'request')
        bat.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        bat.cache = None
        bat.authorizations = []

        m.ReplayAll()

        def callback(url, subresponse, subcontent):
            self.subresponse = subresponse
            self.subcontent  = subcontent

        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callback=callback)

        self.assertRaises(NonBatchResponseError, bat.complete_batch)

        m.VerifyAll()

    def test_multi(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="foomfoomfoom"',
        })
        content = """wah-ho, wah-hay

--foomfoomfoom
Content-Type: application/http-response
Multipart-Request-ID: 2

200 OK
Content-Type: application/json

{"name": "drang"}
--foomfoomfoom
Content-Type: application/http-response
Multipart-Request-ID: 1

200 OK
Content-Type: application/json

{"name": "sturm"}
--foomfoomfoom--"""

        self.headers, self.body = None, None

        bat = BatchClient(endpoint="http://127.0.0.1:8000/")

        m = mox.Mox()
        m.StubOutWithMock(bat, 'request')
        bat.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        bat.cache = None
        bat.authorizations = []

        m.ReplayAll()

        def callbackMoose(url, subresponse, subcontent):
            self.subresponseMoose = subresponse
            self.subcontentMoose  = subcontent
        def callbackFred(url, subresponse, subcontent):
            self.subresponseFred = subresponse
            self.subcontentFred  = subcontent

        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callbackMoose)
        bat.batch({'uri': 'http://example.com/fred'},  callbackFred)
        bat.complete_batch()

        self.assertEquals(self.subcontentMoose, '{"name": "sturm"}')
        self.assertEquals(self.subcontentFred,  '{"name": "drang"}')

        m.VerifyAll()

    def test_not_found(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="foomfoomfoom"',
        })
        content = """wah-ho, wah-hay

--foomfoomfoom
Content-Type: application/http-response
Multipart-Request-ID: 2

200 OK
Content-Type: application/json

{"name": "drang"}
--foomfoomfoom
Content-Type: application/http-response
Multipart-Request-ID: 1

404 Not Found
Content-Type: application/json

{"oops": null}
--foomfoomfoom--"""

        self.headers, self.body = None, None

        bat = BatchClient(endpoint="http://127.0.0.1:8000/")

        m = mox.Mox()
        m.StubOutWithMock(bat, 'request')
        bat.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        bat.cache = None
        bat.authorizations = []

        m.ReplayAll()

        def callback_moose(url, subresponse, subcontent):
            self.subresponseMoose = subresponse
            self.subcontentMoose  = subcontent

            # We might convert an errorful subresponse into an exception in
            # a callback, so check that exceptions that are thrown from the
            # callback percolate out.
            raise httplib.HTTPException('404 Not Found')

        def callback_fred(url, subresponse, subcontent):
            self.subresponseFred = subresponse
            self.subcontentFred  = subcontent

        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callback_moose)
        bat.batch({'uri': 'http://example.com/fred'},  callback_fred)

        self.assertRaises(httplib.HTTPException, lambda: bat.complete_batch() )

        self.assertEquals(self.subresponseMoose.status, 404)
        self.assertEquals(self.subcontentMoose, '{"oops": null}')

        # Does fred still exist? Should it?
        self.assertEquals(self.subcontentFred, '{"name": "drang"}')

        m.VerifyAll()

    def test_cacheful(self):

        response = {
            'content-type': 'multipart/parallel; boundary="=={{[[ ASFDASF ]]}}=="',
        }
        content  = """OMG HAI

--=={{[[ ASFDASF ]]}}==
Content-Type: application/http-response
Multipart-Request-ID: 1

304 Not Modified
Content-Type: application/json
Etag: 7

--=={{[[ ASFDASF ]]}}==--"""

        self.body, self.headers = None, None

        bat = BatchClient(endpoint="http://127.0.0.1:8000/")

        m = mox.Mox()

        mr = m.CreateMock(httplib.HTTPResponse)
        mr.read().AndReturn(content)
        mr.getheaders().AndReturn(response.iteritems())
        mr.status = 207
        mr.reason = 'Multi-Status'
        mr.version = 'HTTP/1.1'

        mc = m.CreateMock(httplib.HTTPConnection)
        mc.request('POST', '/batch-processor', self.mocksetter('body'), self.mocksetter('headers'))
        mc.getresponse().AndReturn(mr)

        bat.connections = {'http:127.0.0.1:8000': mc}

        bat.cache = m.CreateMock(httplib2.FileCache)
        bat.cache.get('http://example.com/moose').AndReturn("""status: 200\r
content-type: application/json\r
content-location: http://example.com/moose\r
etag: 7\r
\r
{"name": "Potatoshop"}""")
        bat.cache.get('http://127.0.0.1:8000/batch-processor').AndReturn(None)
        bat.cache.delete('http://127.0.0.1:8000/batch-processor')
        bat.cache.get('http://example.com/moose').AndReturn("""status: 200\r
content-type: application/json\r
content-location: http://example.com/moose\r
etag: 7\r
\r
{"name": "Potatoshop"}""")
        bat.cache.set('http://example.com/moose', """status: 304\r
etag: 7\r
content-type: application/json\r
content-location: http://example.com/moose\r
\r
{"name": "Potatoshop"}""")

        m.ReplayAll()

        def callback(url, subresponse, subcontent):
            self.subresponse = subresponse
            self.subcontent  = subcontent

        self.assert_(bat.cache)
        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callback)
        bat.complete_batch()

        m.VerifyAll()

        # We captured headers after httplib2 processed them for once, so
        # they're already lowercase and there's a User-Agent header.
        headers = sorted([h for h in self.headers.keys()])
        self.assertEquals(headers, ['accept-encoding', 'content-type', 'mime-version', 'user-agent'])
        self.assertEquals(self.headers['mime-version'], '1.0')

        self.assertEquals(self.subcontent, '{"name": "Potatoshop"}')

    @utils.todo
    def test_authorizations(self):
        raise NotImplementedError()

    def test_length(self):

        keep = lambda: None

        bat = BatchClient(endpoint="http://example.com/")
        bat.batch_request()
        bat.batch({'uri': 'http://example.com/tiny'}, keep)
        bat.batch({'uri': 'http://example.com/small'}, keep)
        bat.batch({'uri': 'http://example.com/medium'}, keep)

        self.assertEquals(len(bat.batchrequest), 3)

        toss = lambda: None
        bat = BatchClient(endpoint="http://example.com/")
        bat.batch_request()
        bat.batch({'uri': 'http://example.com/tiny'}, keep)
        bat.batch({'uri': 'http://example.com/small'}, toss)
        bat.batch({'uri': 'http://example.com/medium'}, toss)
        bat.batch({'uri': 'http://example.com/large'}, keep)
        bat.batch({'uri': 'http://example.com/huge'}, toss)
        del toss

        self.assertEquals(len(bat.batchrequest), 2)

        toss = lambda: None
        bat = BatchClient(endpoint="http://example.com/")
        bat.batch_request()
        bat.batch({'uri': 'http://example.com/tiny'}, toss)
        bat.batch({'uri': 'http://example.com/small'}, toss)
        del toss

        self.assertEquals(len(bat.batchrequest), 0)

    def test_batch_client_errors(self):

        bat = BatchClient(endpoint="http://127.0.0.1:8000/")
        self.assertRaises(BatchError, lambda: bat.complete_batch() )

        self.assertRaises(BatchError, lambda: bat.batch({'uri': 'http://example.com/tiny'}, lambda: None))

        bat.batch_request()
        self.assertRaises(BatchError, lambda: bat.batch_request() )

        bat = BatchClient()
        bat.batch_request()
        bat.batch({'uri': 'http://example.com/tiny'}, lambda: None)
        self.assertRaises(BatchError, lambda: bat.complete_batch() )


# Try including our "with" syntax tests, but skip them if we're in 2.4 where
# "with" syntax is unavailable.
try:
    from tests.client_with import TestBatchRequestsWithSyntax
except SyntaxError:
    class TestBatchRequestsWithSyntax(unittest.TestCase):
        def test_with(self):
            raise nose.SkipTest('No "with" statement in this version of Python')


if __name__ == '__main__':
    utils.log()
    unittest.main()
