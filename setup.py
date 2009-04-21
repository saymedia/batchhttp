#!/usr/bin/env python
from setuptools import setup
setup(
    name='batchhttp',
    version='1.0',
    description='HTTP Request Batching',
    packages=['batchhttp'],
    package_dir={'batchhttp': '.'},

    install_requires=['httplib2>=0.4.0'],
    provides=['remoteobjects'],

    author='Six Apart',
    author_email='python@sixapart.com',
    url='http://code.sixapart.com/svn/remoteobjects/',
)
