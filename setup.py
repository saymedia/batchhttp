#!/usr/bin/env python
from distutils.core import setup
setup(
    name='batchhttp',
    version='1.0',
    description='HTTP Request Batching',
    author='Six Apart',
    author_email='python@sixapart.com',
    url='http://sixapart.github.com/batchhttp/',

    packages=['batchhttp'],
    provides=['batchhttp'],
    requires=['httplib2(>=0.4.0)'],
)
