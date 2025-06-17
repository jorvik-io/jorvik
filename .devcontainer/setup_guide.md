# How to set up Dev container in VS Code
1. Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or the free alternative [Rancher Desktop](https://rancherdesktop.io/)). 
    - After installation, start the container software. Ensure that it is running in the background throughout your development
2. Install the "Dev Containers" extension in VS Code
3. Open project folder in VS Code
4. Open the command palette **Ctrl+Shift+P** on Windows or **Cmd+Shift+P** on Mac
5. Build and open VS Code in Dev container:
    - For the first run, type and select **Dev Containers: Rebuild and Reopen in Container**
    - For subsequent runs, select **Dev Containers: Reopen in Container**
        - Make sure Docker Desktop is running and Dev container started
6. If everything is set up correctly, bottom left corner of VS Code should show <span style="background-color: green; color: white;">>< Pyspark Dev Environment</span>