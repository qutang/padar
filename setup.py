
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
        'pathos',
        'transforms3d',
        'dash',
        'dash-renderer',
        'dash-html-components',
        'dash-core-components',
        'dash-table-experiments',
        'plotly'
    ],
    entry_points='''
        [console_scripts]
        mh=mhealth.cli:main
    ''',
)
