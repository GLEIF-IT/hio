#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
Build and verify release artifacts before publishing:

$ python -m build
$ python -m twine check dist/*
$ python -m twine upload dist/*


Update sphinx /docs
$ cd /docs

$ sphinx-build -b html source build/html
or
$ sphinx-apidoc -f -o source/ ../src/
$ make html

$ git tag # lists all tags
$ git tag -a v0.6.11 -m "new feature"
$ git show v0.6.11
$ git push --tags # pushes tags to default remote
$ git push ioflo --tags   # pushes tags to ioflo remote


Best practices for setup.py and requirements.txt
https://caremad.io/posts/2013/07/setup-vs-requirement/
"""


from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import find_packages
from setuptools import setup



setup(
    name='gleif_hio',
    version='0.6.20rc3',  # also change in src/hio/__init__.py
    license='Apache Software License 2.0',
    description='GLEIF-maintained HIO release for KERI infrastructure',
    long_description=("GLEIF-maintained release of the HIO hierarchical "
                      "concurrency and asynchronous I/O library. The PyPI "
                      "distribution is named gleif_hio and intentionally "
                      "continues to provide the hio import package."),
    long_description_content_type='text/plain',
    author='Samuel M. Smith',
    author_email='smith.samuel.m@gmail.com',
    url='https://github.com/GLEIF-IT/hio',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: Implementation :: CPython',
        # uncomment if you test on these interpreters:
        #'Programming Language :: Python :: Implementation :: PyPy',
        # 'Programming Language :: Python :: Implementation :: IronPython',
        # 'Programming Language :: Python :: Implementation :: Jython',
        # 'Programming Language :: Python :: Implementation :: Stackless',
        'Topic :: Utilities',
    ],
    project_urls={
        'Documentation': 'https://hio.readthedocs.io/',
        'Source': 'https://github.com/GLEIF-IT/hio',
        'Issue Tracker': 'https://github.com/GLEIF-IT/hio/issues',
        'Upstream': 'https://github.com/ioflo/hio',
    },
    keywords=[ "hierarchical concurrency",
               "async io",
               "rich contextual concurrency",
               "structured concurrency",
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires='>=3.12.2',
    install_requires=[
        'lmdb>=1.6.2',
        'msgpack>=1.1.0',
        'cbor2>=5.6.5',
        'multidict>=6.1.0',
        'falcon>=4.0.2',
        'ordered-set>=4.1.0',

    ],
    extras_require={
        # eg:
        #   'rst': ['docutils>=0.11'],
        #   ':python_version=="2.6"': ['argparse'],
    },
    tests_require=[
                    'coverage>=7.6.10',
                    'pytest>=8.3.4',
                  ],
    setup_requires=[
    ],
    entry_points={
        'console_scripts': [
            'hio = hio.cli:main',
            'hiod = hio.daemon:main'
        ]
    },
)
