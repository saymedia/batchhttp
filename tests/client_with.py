from __future__ import with_statement

import unittest

import httplib2
import mox

import batchhttp.client
from batchhttp.client import BatchClient
from tests import utils


class TestBatchRequestsWithSyntax(unittest.TestCase):

    def mocksetter(self, key):
        def mockset(x):
            setattr(self, key, x)
            return True
        return mox.Func(mockset)

    def test_with(self):

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
            self.subresponseWith = subresponse
            self.subcontentWith  = subcontent

        # Try using "with" syntax here.
        with bat.batch_request() as request:
            self.assert_(request is bat.batchrequest)
            bat.batch({'uri': 'http://example.com/moose'}, callback=callback)

        # Make sure the request happened.
        m.VerifyAll()

        self.assert_(hasattr(self, 'subresponseWith'))
        self.assert_(hasattr(self, 'subcontentWith'))


if __name__ == '__main__':
    utils.log()
    unittest.main()
