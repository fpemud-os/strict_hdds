#!/usr/bin/env python3

import sys
import distutils.util
try:
    # First try to load most advanced setuptools setup.
    from setuptools import setup
except:
    # Fall back if setuptools is not installed.
    from distutils.core import setup

# check linux platform
platform = distutils.util.get_platform()
if not platform.startswith('linux'):
    sys.stderr.write("This module is not available on %s\n" % platform)
    sys.exit(1)

# Do setup
setup(
    name='strict_hdds',
    version='0.0.1',
    description='Ensures only some optimized harddisk layouts are used.',
    author='Fpemud',
    author_email='fpemud@sina.com',
	maintainer='Fpemud',
	maintainer_email='fpemud@sina.com',
    url='http://github.com/fpemud/strict_hdds',
    download_url='',
    license='GPLv3 License',
    platforms='Linux',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: GPLv3 License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=['strict_hdds'],
    package_dir={
        'strict_hdds': 'python3/strict_hdds',
    },
)
