import codecs

from typing import List
from setuptools import setup, find_packages


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
    name='grizzly',
    version='4.0.0',
    description='Traffic generator based on locust and behave',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/Biometria-se/grizzly',
    author='Mikael Göransson',
    author_email='github@mgor.se',
    license='MIT',
    packages=find_packages(exclude=['*tests', '*tests.*']),
    python_requires='>=3.8',
    install_requires=install_requires(),
    extras_require={
        'mq': ['pymqi==1.11.0']
    }
)
