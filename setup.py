from setuptools import setup





setup(
    name='Amazon Review Reviewer',
    version='0.1',
    packages=['azquery'],
    entry_points={
        'console_scripts': [
        'azquery = azquery.cli:main',
        ],
    },
    install_requires=[
        'pymongo',
        'click'
    ]
)