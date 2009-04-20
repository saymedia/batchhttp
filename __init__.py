"""

batchhttp provides parallel fetching of HTTP resources through MIME multipart
encoding.

This package's `BatchClient` applies standard MIME multipart encoding to HTTP
messages, providing a standards-conservant technique for making parallelizable
HTTP requests over a single proxy connection.

To make a batch request, open a new request on a `BatchClient` instance and
add your subrequests, along with callbacks that will receive the subresponses.
Once all parallelized requests are added, complete the request; the request is
made and the batched subresponses are provided to your callbacks.

"""

__version__ = '1.0'
__date__ = '20 April 2009'
__author__ = 'Six Apart, Ltd.'
__credits__ = """Brad Choate
Mike Malone
Mark Paschal"""
