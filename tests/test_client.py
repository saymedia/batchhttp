from __future__ import with_statement

import unittest
import logging
import httplib
import httplib2
import re
import mox
import email
import email.message

from remoteobjects import tests, fields, RemoteObject
import batchhttp.client
from batchhttp.client import BatchError, BatchClient

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
Content-Type: message/http-response
Multipart-Request-ID: 1

200 OK
Content-Type: application/json

{"name": "Potatoshop"}
--=={{[[ ASFDASF ]]}}==--"""

        self.body, self.headers = None, None

        http = mox.MockObject(httplib2.Http)
        http.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        http.cache = None
        http.authorizations = []

        mox.Replay(http)

        def callback(subresponse, subcontent):
            self.subresponse = subresponse
            self.subcontent  = subcontent

        bat = BatchClient(http=http, endpoint='http://127.0.0.1:8000/batch-processor')
        bat.batch_request()
        bat.add({'uri': 'http://example.com/moose'}, callback=callback)
        bat.complete_request()

        mox.Verify(http)

        self.assert_(self.headers is not None)
        self.assertEquals(sorted(self.headers.keys()), ['Content-Type', 'MIME-Version'])
        self.assertEquals(self.headers['MIME-Version'], '1.0')

        # Parse the headers through email.message to test the Content-Type value.
        mess = email.message.Message()
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
        self.assertEquals(subresp_msg.get_content_type(), 'message/http-request')
        self.assert_('Multipart-Request-ID' in subresp_msg)

        self.assertEquals(self.subcontent, '{"name": "Potatoshop"}')

    def testMulti(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="foomfoomfoom"',
        })
        content = """wah-ho, wah-hay

--foomfoomfoom
Content-Type: message/http-response
Multipart-Request-ID: 2

200 OK
Content-Type: application/json

{"name": "drang"}
--foomfoomfoom
Content-Type: message/http-response
Multipart-Request-ID: 1

200 OK
Content-Type: application/json

{"name": "sturm"}
--foomfoomfoom--"""

        self.headers, self.body = None, None

        http = mox.MockObject(httplib2.Http)
        http.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        http.cache = None
        http.authorizations = []

        mox.Replay(http)

        def callbackMoose(subresponse, subcontent):
            self.subresponseMoose = subresponse
            self.subcontentMoose  = subcontent
        def callbackFred(subresponse, subcontent):
            self.subresponseFred = subresponse
            self.subcontentFred  = subcontent

        bat = BatchClient(http=http)
        bat.batch_request()
        bat.add({'uri': 'http://example.com/moose'}, callbackMoose)
        bat.add({'uri': 'http://example.com/fred'},  callbackFred)
        bat.complete_request()

        self.assertEquals(self.subcontentMoose, '{"name": "sturm"}')
        self.assertEquals(self.subcontentFred,  '{"name": "drang"}')

        mox.Verify(http)

    def testNotFound(self):

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="foomfoomfoom"',
        })
        content = """wah-ho, wah-hay

--foomfoomfoom
Content-Type: message/http-response
Multipart-Request-ID: 2

200 OK
Content-Type: application/json

{"name": "drang"}
--foomfoomfoom
Content-Type: message/http-response
Multipart-Request-ID: 1

404 Not Found
Content-Type: application/json

{"oops": null}
--foomfoomfoom--"""

        self.headers, self.body = None, None

        http = mox.MockObject(httplib2.Http)
        http.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        http.cache = None
        http.authorizations = []

        mox.Replay(http)

        def callbackMoose(subresponse, subcontent):
            self.subresponseMoose = subresponse
            self.subcontentMoose  = subcontent

            # We might convert an errorful subresponse into an exception in
            # a callback, so check that exceptions that are thrown from the
            # callback percolate out.
            raise httplib.HTTPException('404 Not Found')

        def callbackFred(subresponse, subcontent):
            self.subresponseFred = subresponse
            self.subcontentFred  = subcontent

        bat = BatchClient(http=http)
        bat.batch_request()
        bat.add({'uri': 'http://example.com/moose'}, callbackMoose)
        bat.add({'uri': 'http://example.com/fred'},  callbackFred)

        self.assertRaises(httplib.HTTPException, lambda: bat.complete_request() )

        self.assertEquals(self.subresponseMoose.status, 404)
        self.assertEquals(self.subcontentMoose, '{"oops": null}')

        # Does fred still exist? Should it?
        self.assertEquals(self.subcontentFred, '{"name": "drang"}')

        mox.Verify(http)

    def testCacheful(self):

        class Tiny(RemoteObject):
            name = fields.Something()

        response = httplib2.Response({
            'status': '207',
            'content-type': 'multipart/parallel; boundary="=={{[[ ASFDASF ]]}}=="',
        })
        content  = """OMG HAI

--=={{[[ ASFDASF ]]}}==
Content-Type: message/http-response
Multipart-Request-ID: 1

304 Not Modified
Content-Type: application/json
Etag: 7

--=={{[[ ASFDASF ]]}}==--"""

        self.body, self.headers = None, None

        http = mox.MockObject(httplib2.Http)
        http.request(
            'http://127.0.0.1:8000/batch-processor',
            method='POST',
            headers=self.mocksetter('headers'),
            body=self.mocksetter('body'),
        ).AndReturn((response, content))
        http.authorizations = []

        http.cache = mox.MockObject(httplib2.FileCache)
        http.cache.get('http://example.com/moose').AndReturn("""status: 200\r
content-type: application/json\r
content-location: http://example.com/moose\r
etag: 7\r
\r
{"name": "Potatoshop"}""")
        http.cache.get('http://example.com/moose').AndReturn("""status: 200\r
content-type: application/json\r
content-location: http://example.com/moose\r
etag: 7\r
\r
{"name": "Potatoshop"}""")
        http.cache.set('http://example.com/moose', """status: 304\r
etag: 7\r
content-type: application/json\r
content-location: http://example.com/moose\r
\r
{"name": "Potatoshop"}""")

        mox.Replay(http, http.cache)

        client = BatchClient()
        client.http = http
        self.assert_(http.cache)
        client.batch_request()
        t = Tiny.get('http://example.com/moose', http=http)
        client.add(t)
        client.complete_request()

        mox.Verify(http, http.cache)

        self.assertEquals(sorted(self.headers.keys()), ['Content-Type', 'MIME-Version'])
        self.assertEquals(self.headers['MIME-Version'], '1.0')

        self.assertEquals(t.name, 'Potatoshop')

    @tests.todo
    def testAuthorizations(self):
        raise NotImplementedError()

    def testBatchClientErrors(self):

        client = BatchClient()

        self.assertRaises(BatchError, lambda: client.complete_request() )

        class Tiny(RemoteObject):
            pass

        t = Tiny.get('http://example.com/tinytiny')
        tests.todo(lambda: self.assertRaises(BatchError, lambda: client.add(t) )

        client.batch_request()
        self.assertRaises(BatchError, lambda: client.batch_request() )


if __name__ == '__main__':
    tests.log()
    unittest.main()
