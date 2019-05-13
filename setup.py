#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-stripe",
    version="1.2.6",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stripe"],
    install_requires=[
        "singer-python==5.5.1",
        "stripe==2.10.1",
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.1.1',
            'astroid==2.1.0',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-stripe=tap_stripe:main
    """,
    packages=["tap_stripe"],
    package_data = {
        "schemas": ["tap_stripe/schemas/*.json"]
    },
    include_package_data=True,
)
