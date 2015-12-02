#!/usr/bin/env python

import imp
import os
from setuptools import setup, find_packages

SRC_DIR = 'src'
CSV_UPLOADER_PKG_DIR = os.path.join(SRC_DIR, 'csvuploader')

version = imp.load_source('version', os.path.join(CSV_UPLOADER_PKG_DIR, 'version.py'))

with open('readme.md') as f:
    readme = f.read()

setup(name='CsvUploader',
      version=version.VERSION_STRING,
      description='Shell utility to upload CSVs to a database',
      long_description=readme,
      author='Ed Parcell',
      author_email='edparcell@gmail.com',
      url='',
      package_dir={'': SRC_DIR},
      packages=find_packages(SRC_DIR),
      requires=['argh', 'appdirs', 'py', 'logbook']
      )