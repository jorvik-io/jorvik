# Jorvik
[![Build](https://github.com/GiorgosPa/jorvik/actions/workflows/build.yaml/badge.svg)](https://github.com/GiorgosPa/jorvik/actions/workflows/build.yaml)
[![Coverage Status](https://coveralls.io/repos/github/jorvik-io/jorvik/badge.svg?branch=main)](https://coveralls.io/github/jorvik-io/jorvik?branch=main)
[![PyPI Downloads](https://static.pepy.tech/badge/jorvik)](https://pepy.tech/projects/jorvik)

Jorvik is a collection of utilities for creating and managing ETL pipeline in Pyspark. Build from Data Engineers for Data Engineers.

## Contribute
The Jorvik project welcomes your expertise and enthusiasm!

Writing code isn’t the only way to contribute. You can also:

- review pull requests
- suggest improvements through issues
- let us know your pain-points and repetitive tasks
- help us stay on top of new and old issues
- develop tutorials, videos, presentations, and other educational materials

See [How to Contribute](https://github.com/jorvik-io/jorvik/blob/main/CONTRIBUTING.md) for instructions on setting up your local machine and opening your first Pull Request.

## Getting Started.
Jorvik is available in Pypi and can be installed with pip

```bash
pip install jorvik
```

### Packages:
- [Storage](https://github.com/jorvik-io/jorvik/blob/main/jorvik/storage/README.md): Interact with the storage layer
- [Pipelines](https://github.com/jorvik-io/jorvik/blob/main/jorvik/pipelines/README.md): Build and test etl pipelines with ease
- [Data Lineage](https://github.com/jorvik-io/jorvik/blob/main/jorvik/data_lineage/README.md): Track data lineage

### Examples:
See the full power of jorvik when all the features come together in the examples bellow:
#### Databricks

- [Transactions](https://github.com/jorvik-io/jorvik/blob/main/examples/databricks/transactions/README.md): A multi step pipeline that creates customer statistics from customers and transaction data.
