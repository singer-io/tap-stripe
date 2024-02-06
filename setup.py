#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-stripe",
    version="3.2.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stripe"],
    install_requires=[
        "singer-python==6.0.0",
        "stripe==5.5.0",
    ],
    extras_require={
        'test': [
            'pylint',
            'nose2',
            'coverage'
        ],
        'dev': [
            'ipdb',
            'pylint',
            'astroid==2.5.1',
            'nose2',
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
