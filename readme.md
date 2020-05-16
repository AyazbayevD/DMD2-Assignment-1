# Assignment 1 of DMD 2
> by Ayazbayev Danat, BS-18-03
## What was used
```
Visual Studio Code + Node.js v10.16.3
PostgreSQL 12.2
MongoDB v4.2.3
```
## Needed libraries
For everything to work fine you need to install these libraries by following commands:
```
npm install convert-array-to-csv
npm install file-system
npm install mongodb
npm install pg
```

## Notes
All query scripts, results of queries, and .json representations of collections lie in one folder for the convenience.
Codes in query scripts are properly commented, details of implementation could be known    from there.
## How to use
Make sure that PostgreSQL database is filled properly.
All files from `Scripts` folder in submission archive should lie in one working directory (`.json` files, `.csv` files could be emptied, but not deleted).
Execute `migrator.js`.
Import all `.json` files which are collection names (not results of query executions) into MongoDB using `mongoimport` command which is described in `.pdf` report.
Now you can execute any script, e.g. `query3.js`.
### Any script can be executed by command:
```
node [scriptname].js
```
E.g. command to run migrator:
```
node migrator.js
```
