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
from batchhttp.client import BatchError, BatchClient
from tests import utils


class TestBatchRequests(unittest.TestCase):

    def mocksetter(self, key):
        def mockset(x):
            setattr(self, key, x)
            return True
        return mox.Func(mockset)

    def testLeast(self):

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
        self.assertEquals(sorted(self.headers.keys()), ['Content-Type', 'MIME-Version'])
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

    def testMulti(self):

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

        bat = BatchClient()

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

    def testNotFound(self):

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

        bat = BatchClient()

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

            # We might convert an errorful subresponse into an exception in
            # a callback, so check that exceptions that are thrown from the
            # callback percolate out.
            raise httplib.HTTPException('404 Not Found')

        def callbackFred(url, subresponse, subcontent):
            self.subresponseFred = subresponse
            self.subcontentFred  = subcontent

        bat.batch_request()
        bat.batch({'uri': 'http://example.com/moose'}, callbackMoose)
        bat.batch({'uri': 'http://example.com/fred'},  callbackFred)

        self.assertRaises(httplib.HTTPException, lambda: bat.complete_batch() )

        self.assertEquals(self.subresponseMoose.status, 404)
        self.assertEquals(self.subcontentMoose, '{"oops": null}')

        # Does fred still exist? Should it?
        self.assertEquals(self.subcontentFred, '{"name": "drang"}')

        m.VerifyAll()

    def testCacheful(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="=={{[[ ASFDASF ]]}}=="',
        })
        content  = """OMG HAI

--=={{[[ ASFDASF ]]}}==
Content-Type: application/http-response
Multipart-Request-ID: 1

304 Not Modified
Content-Type: application/json
Etag: 7

--=={{[[ ASFDASF ]]}}==--"""

        self.body, self.headers = None, None

        bat = BatchClient()

        m = mox.Mox()
        m.StubOutWithMock(bat, 'request')
        bat.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        bat.authorizations = []

        bat.cache = m.CreateMock(httplib2.FileCache)
        bat.cache.get('http://example.com/moose').AndReturn("""status: 200\r
content-type: application/json\r
content-location: http://example.com/moose\r
etag: 7\r
\r
{"name": "Potatoshop"}""")
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

        self.assertEquals(sorted(self.headers.keys()), ['Content-Type', 'MIME-Version'])
        self.assertEquals(self.headers['MIME-Version'], '1.0')

        self.assertEquals(self.subcontent, '{"name": "Potatoshop"}')

    @utils.todo
    def testAuthorizations(self):
        raise NotImplementedError()

    def testBatchClientErrors(self):

        bat = BatchClient()
        self.assertRaises(BatchError, lambda: bat.complete_batch() )

        self.assertRaises(BatchError, lambda: bat.batch({'uri': 'http://example.com/tiny'}, lambda: None))

        bat.batch_request()
        self.assertRaises(BatchError, lambda: bat.batch_request() )


try:
    from tests.client_with import TestBatchRequestsWithSyntax
except SyntaxError:
    class TestBatchRequestsWithSyntax(unittest.TestCase):
        def testWith(self):
            raise nose.SkipTest('No "with" statement in this version of Python')


if __name__ == '__main__':
    utils.log()
    unittest.main()
