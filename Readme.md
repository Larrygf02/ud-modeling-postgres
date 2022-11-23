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
