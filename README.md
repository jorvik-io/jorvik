# Jorvik
Jorvik is a collection of utilities for creating and managing ETL pipeline in pyspark.

# Development 
**_NOTE:_**  JAVA 11 or JAVA 17 is required. On a Mac you can install with `brew install openjdk@17`.

Setup the package in editable mode including the dependencies needed for testing.
`pip install -e '.[tests]'`

## Editor
VS Code is the recommended editor and the project comes with the VScode settings that follow the project guidelines. See [.vscode/settings.json](.vscode/settings.json).

## Testing
You can run the tests by running the command `pytest test`.

To run the tests in VS code you may need to point to the correct Java version in VScode's python context. To do so add .env file in the root folder and include the JAVA_HOME environment variable for example `JAVA_HOME=/opt/homebrew/opt/openjdk@17`.

## Linting
The project enforces flake8 rules with the following exceptions: 
E302, E305: Expected 2 blank lines
max line length: 127

To ignore flake8 errors you can add the following comment in the affected code line `# noqa: ERRORCODE`.

## Spell checks
Sometimes spelling mistakes cannot be avoided. For example the spelling mistake is a function from a dependent library. you can ignore spelling mistakes by adding the comment `# cspell: words word1 word2` in the top of the file.  You can ignore the words by adding them in [cSpell.json](cspell.json).
