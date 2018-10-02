#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-stripe",
    version="1.0.0",
    description="Singer.io tap for Stripe",
    author="Statsbot",
    url="http://statsbot.co",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_stripe"],
    install_requires=[
        "singer-python==5.2.0",
        "stripe==2.4.0",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-stripe=tap_stripe:main
    """,
    packages=["tap_stripe"],
    package_data = {
        "schemas": ["tap_stripe/schemas/*.json", "tap_stripe/schemas/shared/*.json"]
    },
    include_package_data=True,
)
