#!/usr/bin/env python
# -*- coding: utf-8 -*-


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os
import re

with open("README.md", "r", encoding="utf8") as fh:
    readme = fh.read()

package = "datagun"


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    init_py = open(os.path.join(package, "__init__.py")).read()
    return re.search("^__version__ = ['\"]([^'\"]+)['\"]", init_py, re.MULTILINE).group(1)


setup(
    name="datagun",
    version=get_version(package),
    description="Data converter",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Pavel Maksimov",
    author_email="vur21@ya.ru",
    url="https://github.com/pavelmaksimov/python-data-transformer",
    packages=[package],
    include_package_data=True,
    install_requires=["dateutil"],
    extras_require={
        "pandas": ["pandas"]
    },
    license="MIT",
    zip_safe=False,
    keywords="datagun,data,transform,transformer,deserialize,serialize",
    test_suite="tests",
)
