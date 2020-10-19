from setuptools import find_packages, setup

setup(
    name='MingKDD',
    version='1.0.1',
    url='',
    license='',
    maintainer='kael',
    maintainer_email='congshi.hello@gmail.com',
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
