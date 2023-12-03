# Databases Project

Use this repository for your integration code and any source code created while working on your project (ad-hoc code,
SQL queries, project files needed by external tools, etc.).

- Merge your code into the main branch on the due date. 
- Do not commit datasets!
- Any other document (except for the dump in the final hand-in) should be handed-in via ADAM.

If you have any questions regarding the project, please do not hesitate to ask during the exercise lessons or via mail
to [raphael.waltenspuel@unibas.ch](mailto:raphael.waltenspuel@unibas.ch)!

It is recommended that you first create a ```.gitignore``` file. (And exclude the "datasets" folder, for example). A useful tool for creating ```.gitignore``` files is www.gitignore.io.

Feel free to update or replace this readme with a brief description of your project and goals.

### Database setup guide

1. Make sure all the requirements in ’requirements.txt’ are met. If they are not
met, run pip install -r requirements.txt in the root of the project.
2. Run the python script ’integrate.py’ in the ’src’ folder. Set all booleans to
’False’ in the main methode of the script. If the datasets have already been
downloaded, set all the booleans to ’True’. The datasets need to be in a
folder named ’datasets’ in ’src’ (this should be set up automatically by the
script).
3. Ensure you have a running Postgres instance with a database.
4. Ensure you have the correct credentials in the python script ’fill_db.py’ in
’dbinfo’
5. Run ’fill_db.py’