import os
from setuptools import setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="python-vfnm-sdk",
    version="0.0.2",
    author="Open Baton",
    author_email="info@openbaton.org",
    description=("The Python version of the vnfm-sdk"),
    license="Apache 2",
    keywords="python vnfm nfvo open baton openbaton sdk",
    url="http://openbaton.github.io/",
    packages=[
        'utils',
        'etc',
        'interfaces'
    ],
    long_description=read('README.md')
)
