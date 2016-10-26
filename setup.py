from setuptools import setup

setup(
    name='cql-migrate',
    version='1.0',
    py_modules=['cql-migrate'],
    include_package_data=True,
    install_requires=[
        'click',
        'cassandra-driver',
        'futures',
        'invoke',
        'six'
    ],
    entry_points='''
        [console_scripts]
        cql-migrate=migrate:cli
    ''',
)