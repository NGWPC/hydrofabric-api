1.)  Get Docker container from s3://ngwpc-dev/DanielCumpton/hydrofabric_service_v1.tar
2.   Get supporting data from s3://ngwpc-dev/DanielCumpton/hydrofabric_data.tgz

3.)  Start Docker and load docker image:  sudo systemctl start docker  
       sudo docker load -i hydrofabric_service_v1.tar

4.)  Untar hydrofabric_data.tgz in home directory (or wherever you want)

5.)  Get docker image id, create container, and mount hydrofabric data directory:
       sudo docker images  
       sudo docker run -ti -v <root path to where you untarred the hydrofabric data>/Hydrofabric:/Hydrofabric <image id from previous command > bash
                

6.)  Clone hydrofabric_api repo to container. 
                Paths for input data, hydrofabric data, the temp directory, and s3 bucket information are stored in
                config.yml.  You shouldn't need to change any of the paths except for maybe the s3 bucket and/or prefix.  
                 The django endpoints are under djangoApps/init_param_app/views.py
                
7.)  In the terminal where django will be run:
     Set environment variable for GDAL:  export LD_LIBRARY_PATH=/usr/local/lib64
     Copy and paste AWS S3 credentials.  
                 
8.)  Run Django server:  cd <root path>/hydrofabric_api/djangoApps
       python3 manage.py runserver
                
9.)  curl commands for getting the geopackage and for getting initial parameters for CFE-S and CFE-X are in 
     /home/hydrofabric_api/curl.py.
     python3 curl.py          
                 
10.)  Files are written to the s3 bucket and you can see them in the temp directory (/Hydrofabric/data/temp)

## Development Setup with VS Code and Docker (Dev Containers)

This project allows the use of Docker and Visual Studio Code (VS Code) to create a development environment for a Python 3.11 application with PostgreSQL and the intial Hydrofabric base requirements. This guide will walk you through the setup of your development environment using devcontainer.json, docker-compose.yml, and Dockerfile.

### Prerequisites
- Docker and Docker Compose installed on your machine.
- Visual Studio Code (VS Code) installed with the Remote - Containers extension.
- GitLab credentials
- Hydrofabric Data from: s3://ngwpc-hydrofabric/hydrofabric_data.tgz in a directory at the root of this code repo called: Hydrofabric (It is already in .gitignore, to prevent accidentlaly be committing the code)
- A dump of the hydrofabric_db that can be loaded to your local dev database

### Dev Container Structure
 - devcontainer.json: Configures the VS Code development container environment.
 - docker-compose.yml: Defines the multi-container Docker application, including the app and PostgreSQL database.
 - Dockerfile: Specifies the base image and additional dependencies for the app container.

### Setting Up the Development Environment
 1. devcontainer.json
This file configures the development container for VS Code. It defines which Docker services to use and customizes the VS Code environment.
       - dockerComposeFile: Path to the docker-compose.yml file.
       - service: The name of the service that VS Code should connect to, in this case, app.
       - workspaceFolder: The working directory inside the container.
       - forwardPorts: Ports to be forwarded from the container to the host.
 2. docker-compose.yml
Defines the services for your application. In this case, it includes the app service and a PostgreSQL database.
       - app service: The main application service built from the Dockerfile. It also mounts local directories into the container.
       - db service: PostgreSQL database with PostGIS extension.
       - volumes: Defines a volume for persisting PostgreSQL data.
3. Dockerfile
Specifies the base image and sets up the Python environment.
       - Base Image: Uses a pre-built image as a base.
       - Dependencies: Upgrades the system and installs Python 3.11 along with necessary packages.
       - Python Alternatives: Sets Python 3.11 as the default Python version.
       - Requirements: Installs Python dependencies listed in requirements.txt.

### Getting Started
1. Open VS Code: Open the project folder in VS Code. I generally cd to the code in my terminal and run:

       code .

2. Rebuild the Container: Open the Command Palette (Ctrl+Shift+P or Cmd+Shift+P) and select Remote-Containers: Rebuild Container. You may also be prompted in the lower right hand corner to reopen in a dev container when your VS Code notices that there is a .devcontainer configuration in the project.
3. Access the Dev Container: Once the rebuild is complete, VS Code will connect to the development container.
4. If you will be using git from within the container, be sure to configure your user name and e-mail.

       git config --global user.name "Your Name"
       git config --global user.email "your.email@example.com"


You should now be able to develop the application within a fully configured development container with all necessary extensions and settings.

### Cleaning up when you run into issues
There are times that devcontainers, in particular when combined with docker-compose can get into a bad state.  I like to start fresh when this occurs by pruning everything and even deleting all the docker volumes, so I can start fresh.
