#!/usr/bin/env python
from setuptools import setup, find_packages

requires = [
    'hieratic>=0.0.6',
    'six>=1.9.0',
    'boto3>=1.4.4',
]

setup(
    name='hieratic_dynamodb',
    version='0.0.7',
    description='dynamodb plugin for hieratic.',
    author='Satoshi Ebihara',
    author_email='kiri@succhiello.net',
    url='https://github.com/succhiello/hieratic-dynamodb',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
    ],
    packages=find_packages(),
    install_requires=requires,
    tests_require=['pytest'],
    entry_points={
        'hieratic.engine': [
            'dynamodb = hieratic_dynamodb',
        ],
    },
)
