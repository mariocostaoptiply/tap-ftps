#!/usr/bin/env python
from setuptools import setup


with open('requirements.txt', 'r') as fh:
    requirements = fh.read().splitlines()

setup(
    name="tap-ftp",
    version="0.0.2",
    description="Singer.io tap for extracting data",
    author="Optiply",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_ftp"],
    install_requires=requirements,
    entry_points="""
    [console_scripts]
    tap-ftp=tap_ftp.tap:main
    """,
    packages=["tap_ftp", "tap_ftp.singer_encodings"]
)
