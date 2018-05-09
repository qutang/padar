
from setuptools import setup, find_packages

setup(
    name='padar',
    version='1.0.9',
    description='Processing Accelerometer Data and Do Activity Recognition',
    long_description_content_type='text/markdown',
    url='https://github.com/qutang/padar',
    author='Qu Tang',
    author_email='tqshelly@gmail.com',
    license='MIT',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development',
        'Topic :: Scientific/Engineering',
        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
    ],
    keywords='data mhealth accelerometer machine learning',
    project_urls={
        'Source': 'https://github.com/qutang/padar/',
        'Tracker': 'https://github.com/qutang/padar/issues',
    },
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click',
        'jinja2',
        'pweave',
        'pandas',
        'numpy',
        'scipy',
        'pathos',
        'transforms3d',
        'scikit-learn',
        'altair'
    ],
    python_requires='>=3',
    entry_points={
        'console_scripts': [
            'pad=padar.pad:main',
            'dar=padar.dar:main',
            'mdcas=padar.mdcas:main',
            'padar=padar.helper:main'
        ]
    },  
)
