from setuptools import setup, find_packages

version='0.1'

packages = find_packages(exclude=['ez_setup', 'examples', 'tests'])
print packages
setup(
    name='mongodbtools',
    version=version,
    description='Python tools for working with MongoDB',
    author='Jason Wilder',
    author_email='code@jasonwilder.com',
    maintainer='Jason Wilder',
    license='MIT',
    url='http://github.com/jwilder/mongodb-tools',
    packages=packages,
    entry_points = """\
    [console_scripts]
    collection-stats=mongodbtools.collection_stats:main
    index-stats=mongodbtools.index_stats:main
    redundant-indexes=mongodbtools.redundant_indexes:main
    mongo-sniff=mongodbtools.mongo_sniff:main
    """,
    install_requires=[
        'pymongo>=2.3',
        'PrettyTable==0.5.0',
        'psutil==0.3.0',
        'mongoengine==0.6.0',
        'colorama==0.2.4',
        'pcapy==0.10.5',
        'impacket==0.9.6.0'
    ],
)
