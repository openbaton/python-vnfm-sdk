import os

from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="python-vnfm-sdk",
        version='5.2.1',
    author="Open Baton",
    author_email="dev@openbaton.org",
    description="The Python version of the Open Baton vnfm-sdk",
    license="Apache 2",
    keywords="python vnfm nfvo open baton openbaton sdk",
    url="http://openbaton.github.io/",
    packages=find_packages(),
        install_requires=[
            'pika',
            'futures'  # for py2
        ],
    long_description=read('README.rst'),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        'Topic :: Software Development :: Build Tools',
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ]
)
