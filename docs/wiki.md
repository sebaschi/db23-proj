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

# Load csv into db HOT FIX
Go to directory containing the csvs.
```bash
cd group-1/src/datasets/integrated/ && psql -h localhost -d proj_db -U sebl -p 5433
```
Then manually copy
```postgresql
\copy FootBikeCount FROM 'FootBikeCount.csv' WITH CSV HEADER
\copy mivcount FROM 'MivCount.csv' WITH CSV HEADER
```

# How to create a db dump from the command line
```bash
pg_dump -U sebl -p 5433 -d proj_db > [dump_file].sql
```


