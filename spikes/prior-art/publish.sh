#!/bin/bash

rm -Rf dist
./setup.py sdist bdist_wheel
twine upload dist/*