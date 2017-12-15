
from setuptools import setup, find_packages

setup(
    name='mhealth',
    version='0.0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'jinja2',
        'pweave',
        'pandas',
        'pathos'
    ],
    entry_points='''
        [console_scripts]
        mh=mhealth.cli:main
    ''',
)
