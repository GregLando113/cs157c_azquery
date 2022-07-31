from setuptools import setup





setup(
    name='Amazon Review Reviewer',
    version='0.1',
    packages=['azquery'],
    include_package_data=True,
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