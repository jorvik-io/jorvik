# Contributing
You will need your own copy of jorvik (aka fork) to work on the code. Clone the forked repository locally add your changes and create a Pull Request from the forked repo to jorvik.

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


[Set up your machine](setup.md)


## Creating a Pull Request
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
