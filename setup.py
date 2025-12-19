#!/usr/bin/env python
from setuptools import setup


with open('requirements.txt', 'r') as fh:
    requirements = fh.read().splitlines()

setup(
    name="tap-ftps",
    version="0.0.2",
    description="Singer.io tap for extracting data",
    author="Optiply",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_ftps"],
    install_requires=requirements,
    entry_points="""
    [console_scripts]
    tap-ftps=tap_ftps.tap:main
    """,
    packages=["tap_ftps", "tap_ftps.singer_encodings"]
)
