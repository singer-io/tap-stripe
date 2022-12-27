#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-stripe",
    version="2.0.2",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stripe"],
    install_requires=[
        "singer-python==5.13.0",
        "stripe==2.64.0",
    ],
    extras_require={
        'test': [
            'pylint==2.7.2',
            'nose==1.3.7',
            'coverage'
        ],
        'dev': [
            'ipdb',
            'pylint==2.7.2',
            'astroid==2.5.1',
            'nose==1.3.7'
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
