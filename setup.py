from setuptools import setup

setup(
    name='shift',
    description='A tool for migration management with Cassandra',
    url='http://github.com/rgamba/shift/',
    author='Ricardo Gamba',
    author_email='rgamba@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 1 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    version='0.1',
    py_modules=['cql-migrate'],
    include_package_data=True,
    packages=[
        'shift'
    ],
    install_requires=[
        'click>=6.6',
        'cassandra-driver>=3.7.0',
        'futures>=3.0.5',
        'invoke>=0.13.0',
        'six>=1.10.0'
    ],
    entry_points='''
        [console_scripts]
        shift=shift.migrate:cli
    ''',
)
