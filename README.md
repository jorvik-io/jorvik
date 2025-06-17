# Jorvik
Jorvik is a collection of utilities for creating and managing ETL pipeline in pyspark.

# How to Contribute
The Jorvik project welcomes your expertise and enthusiasm!

Writing code isn’t the only way to contribute. You can also:

- review pull requests
- suggest improvements through issues
- help us stay on top of new and old issues
- develop tutorials, presentations, and other educational materials

## Contributing Code
You will need your own copy of jorvik (aka fork) to work on the code. Clone the forked repository locally add your changes and create a Pull Request from the forked repo to jorvik.

To setup your machine:
- fork the repository
    Go to https://github.com/jorvik-io/jorvik and click the fork button. This will create a copy of jorvik in your Github account https://github.com/your-username/jorvik

- clone your fork in your machine
    ```bash
    git clone https://github.com/your-username/jorvik.git
    ```
- add a reference to jorvik-io/jorvik to easily fetch updates
    ```bash
    git remote add jorvik https://github.com/jorvik-io/jorvik.git
    ```
- check your setup
    ```bash
    git remote -v
    ```
    You should expect to see 2 remote references origin that points to your account and jorvik that point to jorvik-io

To create a Pull Request and submit code:
- checkout main branch
- take the latest changes from jorvik, see also [this article](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork)
    ```bash
    git pull jorvik main
    ```
- create a new branch
    ```bash
    git checkout -b feature-branch
    ```
- commit and push your changes
    ```bash
    git add .
    git commit -m 'Your commit message'
    git push --set-upstream origin feature-branch
    ```
- create a Pull Request from your fork to jorvik
    

Click [here](https://opensource.guide/how-to-contribute/) for more information about contributing to open source projects.


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
