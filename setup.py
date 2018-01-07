#!/usr/bin/env python
from setuptools import setup, find_packages
# configure the setup to install from specific repos and users

DEPENDENCY_LINKS = [
]

DESC ='Python JSON file consumer'
setup(name='json-file-consumer',
      version='1.0',
      description=DESC,
      author='adam pridgen',
      author_email='dso@thecoverofnight.com',
      install_requires=['toml', 'distribute',  'regex'],
      packages=find_packages('src'),
      package_dir={'': 'src'},
      dependency_links=DEPENDENCY_LINKS,
)
