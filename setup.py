import codecs

from setuptools import setup, find_packages

from grizzly import __version__


def long_description() -> str:
    with codecs.open('README.md', encoding='utf-8') as fd:
        return fd.read()


setup(
    name='grizzly-loadtester',
    version=__version__,
    description='Traffic generator based on locust and behave',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    project_urls={
        'Documentation': 'https://biometria-se.github.io/grizzly/',
        'Code': 'https://github.com/biometria-se/grizzly/',
        'Tracker': 'https://github.com/Biometria-se/grizzly/issues',
    },
    url='https://github.com/Biometria-se/grizzly',
    author='Biometria',
    author_email='opensource@biometria.se',
    license='MIT',
    packages=find_packages(exclude=['*tests', '*tests.*']),
    package_data={
        'grizzly': ['py.typed'],
        'grizzly_extras': ['py.typed'],
    },
    python_requires='>=3.8',
    install_requires=[
        'aenum>=3.1.8',
        'azure-core<1.23.0',
        'azure-servicebus>=7.6.0',
        'azure-storage-blob>=12.9.0',
        'behave>=1.2.6',
        'influxdb>=5.3.1',
        'Jinja2>=3.0.3',
        'jsonpath-ng>=1.5.3',
        'locust==2.8.4',
        'lxml>=4.8.0',
        'mypy-extensions>=0.4.3',
        'opencensus-ext-azure>=1.1.1',
        'paramiko>=2.9.2',
        'python-dateutil>=2.8.2',
        'pytz>=2021.3',
        'backports.zoneinfo>=0.2.1 ; python_version < "3.9"',
        'pyzmq>=22.3.0',
        'typing-extensions>=3.10.0<4.0.0',
        'tzlocal>=4.1',
        'PyYAML<6.0.0,>=5.3.0',
        'setproctitle>=1.2.2',
    ],
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
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX :: Linux',
    ],
    extras_require={
        'mq': [
            'pymqi==1.12.0',
        ],
        'dev': [
            'astunparse>=1.6.3',
            'mkdocs>=1.2.0',
            'mkdocs-material>=8.2.1',
            'mypy>=0.931',
            'flake8>=4.0.0',
            'pip-licenses>=3.5.3',
            'pydoc-markdown>=4.6.1,<5.0.0',
            'pylint>=2.12.2',
            'pytablewriter>=0.64.1',
            'pytest>=7.0.0',
            'pytest-cov>=3.0.0',
            'pytest-mock>=3.7.0',
            'pytest-timeout>=2.1.0',
            'types-paramiko>=2.8.13',
            'types-python-dateutil>=2.8.9',
            'types-pytz>=2021.3.5',
            'types-PyYAML<6.0.0,>=5.3.0',
            'types-requests>=2.27.0',
            'types-tzlocal>=4.0.0',
            'types-Jinja2>=2.0.0',
        ],
        'ci': [
            'twine>=3.8.0',
            'wheel>=0.37.1',
            'pip-tools>=6.5.0',
        ]
    },
    entry_points={
        'console_scripts': [
            'async-messaged=grizzly_extras.async_message.daemon:main',
        ]
    },
)
