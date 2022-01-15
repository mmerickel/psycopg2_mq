from setuptools import setup, find_packages

def readfile(name):
    with open(name) as f:
        return f.read()

readme = readfile('README.rst')
changes = readfile('CHANGES.rst')

requires = [
    'python-dateutil',
    'SQLAlchemy',
    'transaction',
    'zope.sqlalchemy',
]

tests_require = [
    'pytest',
    'pytest-cov',
]

setup(
    name='psycopg2_mq',
    version='0.6.1',
    description='A message queue written around PostgreSQL.',
    long_description=readme + '\n\n' + changes,
    author='Michael Merickel',
    author_email='oss@m.merickel.org',
    url='https://github.com/mmerickel/psycopg2_mq',
    packages=find_packages('src', exclude=['tests']),
    package_dir={'': 'src'},
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=requires,
    extras_require={
        'testing': tests_require,
    },
    test_suite='tests',
    zip_safe=False,
    keywords=','.join([
        'psycopg2',
        'postgres',
        'postgresql',
    ]),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
