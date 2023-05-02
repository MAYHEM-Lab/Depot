#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='depot-python-client',
    version='1.0',
    author='KC',
    author_email='me@knc.wtf',
    packages=['depot_client'],
    package_dir={'depot_client': 'src'}
)
