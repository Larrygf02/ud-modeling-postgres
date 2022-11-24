# About project

In this project we are going to model the user activity information in a music startup Sparkify. For this we will use the facts and dimensions table.

Dimension Tables: songs, artists, users, time
Fact Table: songplays

# Files in project

- create_tables.py => Creates the database with dimensional and fact tables
- data => Contains the information to process
- etl.py => Extract the information then transform it data into tables, finally insert into database

# How to run files

```
python create_tables.py
python etl.py
```

# Process ETL

### Function process_song_file:

- Get files of directory
- Read files and concatenate in one dataframe
- Use executemany function to insert all data in one execution

### Function process_log_file:

- Get files of directory
- Read files and concatenate in one dataframe
- Use executemany function to insert all data in one execution

### Function get_files

- Receives as parameter the path
- Lists all paths that have json extension and aggregates them into an array
- Return all paths
