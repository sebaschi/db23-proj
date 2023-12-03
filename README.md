# Database Project Group 1

## Preliminaries
* Ensure you have access to a running postgres instance
* Ensure you have ```python3``` and ```pip``` installed.
* From within the root of the project, run ```pip install -r requirements.txt```. This insures all python dependecies are  met.
* In ```src/fill_db.py``` look for the ```db_info``` variable and adapt it to your credentials.

## Action
In the following the order matters.
1. Run ```ensure_dirs_exist.py```. This makes sure all the directories needed to perform the data integration and logging exist.
1. Run ```integrate.py```. Adjust the main method to fit your needs. In particular adjust the ```process_all_data()``` method, such that the parameter corresponding to a dataset is ```False``` if the script shall download it form the  internet, and ```True``` else. To get geojson data form signaled speed in to city of Zurich uncomment the line in the ``main`` method where you find ```load_tempo_geojson_from_api_to_local()```
2. Run ```fill_db.py```. This will load the data into the database based on the credentials given in the ``db_info`` variable.
3. Perform Analysis.