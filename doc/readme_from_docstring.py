#!/usr/bin/env python

import batchhttp

readme = file('README.rst', 'w')
readme.write(batchhttp.__doc__.strip())
readme.write("\n")
readme.close()
