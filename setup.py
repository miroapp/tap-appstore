#!/usr/bin/env python

from setuptools import setup

setup(name='tap-appstore',
      version='0.2.1',
      description='Singer.io tap for extracting data from the App Store Connect API',
      author='JustEdro',
      url='https://github.com/JustEdro',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap-appstore'],
      install_requires=[
          'singer-python==5.2.3',
          'appstoreconnect==0.9.0',
          'pytz==2018.4'
      ],
      entry_points='''
          [console_scripts]
          tap-appstore=tap_appstore:main
      ''',
      packages=['tap_appstore'],
      package_data={
          'tap_appstore/schemas': [
              'summary_sales_report.json'
          ],
      },
      include_package_data=True,
      )
