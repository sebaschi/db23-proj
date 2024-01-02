# Setup Of Virtual Python dev env
First open the terminal and make sure to be in the root directory. 
All steps assume one is in the root folder. 
## Create Virtual environment
```
python3 -m venv db23-project-venv
```
## Activating the virtual environment
```
source db23/bin/activate
```
#### When in the environment ``db23-project`` just install all needed packages.
```
pip3 install pkg_name
```
## Getting back out
```
deactivate
```

# List of used packages
See ``requirements.txt``

# Setting up postgres
# Setting up pgadmin as container serverd by nginx