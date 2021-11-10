import codecs

from typing import List
from setuptools import setup, find_packages

from grizzly import __version__


def long_description() -> str:
    with codecs.open('README.md', encoding='utf-8') as fd:
        return fd.read()


def install_requires() -> List[str]:
    install_requires: List[str] = []
    with codecs.open('requirements.txt', encoding='utf-8') as fd:
        for line in fd.readlines():
            install_requires.append(line.strip())

    return install_requires


setup(
    name='grizzly-loadtester',
    version=__version__,
    description='Traffic generator based on locust and behave',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/Biometria-se/grizzly',
    author='Biometria',
    author_email='opensource@biometria.se',
    license='MIT',
    packages=find_packages(exclude=['*tests', '*tests.*']),
    python_requires='>=3.8',
    install_requires=install_requires(),
    keywords=[
        'locust',
        'behave',
        'load',
        'loadtest',
        'performance',
        'traffic generator',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX :: Linux',
    ],
    extras_require={
        'mq': ['pymqi==1.11.0']
    },
    entry_points={
        'console_scripts': [
            'async-messaged=grizzly_extras.async_message.daemon:main',
        ]
    },
)
