import os
import re
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    LONG_DESCRIPTION = fh.read()
with open('requirements.txt') as fi:
    REQUIRE = [
        line.strip() for line in fi.readlines()
        if not line.startswith('#')
    ]

if os.path.exists("jorvik/version.py"):
    with open("jorvik/version.py", "r", encoding="utf-8") as fh:
        VERSION = fh.read()
        VERSION = re.findall('"(.*)"', VERSION)[0]
else:
    VERSION = "1.0.0"

setuptools.setup(
    name='jorvik',
    author="https://github.com/jorvik-io",
    version=VERSION,
    description="A set of utilities for creating and managing ETL Pipelines with pyspark.",
    keywords=[
        "ETL",
        "Pyspark",
        "Data Engineering",
        "Data Pipelines"
    ],
    long_description=LONG_DESCRIPTION,
    url="https://github.com/jorvik-io/jorvik",
    license="Apache License 2.0",
    long_description_content_type="text/markdown",
    install_requires=REQUIRE,
    extras_require={'tests': ['pytest', 'flake8', 'pytest-mock', 'numpy<2.0.0', 'ipykernel']},
    data_files=[('', ['requirements.txt'])],
    packages=setuptools.find_packages(''),
)
