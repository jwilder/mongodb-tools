from setuptools import setup, find_packages

version='0.2'

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
    """,
    install_requires=[
        'pymongo>=2.1',
        'PrettyTable>=0.7.1',
        'psutil==0.3.0',
        'mongoengine==0.5.0'
    ],
)
