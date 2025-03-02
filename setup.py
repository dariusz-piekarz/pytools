from setuptools import setup, find_packages


setup(
    name='pytools',
    version='0.0.1',
    author='Dariusz Piekarz',
    author_email='darpiekarz@wp.pl',
    description='Package containing tools related to configuration files, concurrency management, '
                'metaclasses, decorators, SQL connectors, httpx API connectors etc.',
    long_description='',
    url='https://github.com/dariusz-piekarz/pytools/blob/master/pytools/',
    packages=find_packages(),
    install_requires=['aiofiles>=22.1.0',
                      'pyarrow>=19.0.0',
                      'pandas>=1.5.0',
                      'loguru>=0.7.2',
                      'numpy>=1.1.0',
                      'pyodbc>=5.1.0',
                      'psycopg2>=2.9.9',
                      'httpx>=0.27.0'],
    classifiers=['Programming Language :: Python :: 3', 'Operating System :: OS Independent'],
    )
