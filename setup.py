from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    LONG_DESCRIPTION = fh.read()
with open('requirements.txt') as fi:
    REQUIRE = [
        line.strip() for line in fi.readlines()
        if not line.startswith('#')
    ]

setup(
    name='jorvik',
    author="Jorvik",
    version="0.0.1",
    description="A set of utilities for creating and managing up ETL Pipelines with pyspark.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    install_requires=REQUIRE,
    extras_require={'tests': ['pytest', 'flake8', 'pytest-mock', 'numpy<2.0.0']},
    data_files=[('', ['requirements.txt'])],
    packages=find_packages(''),
)
