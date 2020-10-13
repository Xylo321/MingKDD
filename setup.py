from setuptools import find_packages, setup

setup(
    name='reborn-kdd',
    version='1.0.1',
    url='',
    license='',
    maintainer='zswj123',
    maintainer_email='l2se@sina.cn',
    description='',
    long_description='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "selenium",
        "requests",
        "lxml",
        "cssselect",
        "flask"
    ]
)
