# Copyright (c) 2009 Six Apart Ltd.
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
