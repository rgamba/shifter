from setuptools import setup

setup(
    name='cql-migrate',
    description='A tool for migration management with Cassandra',
    author='Ricardo Gamba',
    author_email='rgamba@gmail.com',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
    version='0.1',
    py_modules=['cql-migrate'],
    include_package_data=True,
    packages=[
        'cqlmigrate'
    ],
    install_requires=[
        'click',
        'cassandra-driver',
        'futures',
        'invoke',
        'six'
    ],
    entry_points='''
        [console_scripts]
        cqlmigrate=cqlmigrate.migrate:cli
    ''',
)
