from setuptools import setup

setup(
    name='shifter',
    description='A tool for migration management with Cassandra',
    long_description='shifter gets the pain away from managing migrations with Cassandra',
    url='http://github.com/rgamba/shift/',
    author='Ricardo Gamba',
    author_email='rgamba@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    version='0.1',
    keywords='development database migration cassandra',
    include_package_data=True,
    packages=['shifter'],
    install_requires=[
        'click>=6.6',
        'cassandra-driver>=3.7.0',
        'futures>=3.0.5',
        'invoke>=0.13.0',
        'six>=1.10.0'
        'cqlsh'
    ],
    entry_points={
        'console_scripts': [
            'shifter = shifter.migrate:cli',
        ],
    },
)
