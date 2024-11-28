# Movies in a NOSQL Database

Migrate the movies database from RDBMS to a NoSql database (GraphSQL)

## Answers Organization

- Data exportation in CSV files : /db_migration.py
- Csv files of the corresponding nodes/entities : /GENERATED CSVs/..
  (for the code : csv files are uploaded on Google Drive because Neo4j doesn't work with local paths)
- Script to create the database and populate the data : /db_pop_neo.py
- Queries : /queries.cypher

## Python Config (same configuration as in assignment 2)

#### Install python :

```
brew install python
```

#### Check python's version :

```
python3 --version
```

#### Install request library using PIP :

```
pip3 install requests
```

#### Install Psycopg2

```
brew install postgresql
```

```
pip3 install psycopg2
```

#### Install Postgres

```
brew install postgresql
```

#### Requests library used to fetch the API :

```
pip3 install requests
```

#### Request doesn't work unless a virtual environment is set :

```
python3 -m venv path/to/venv
```

```
python3 -m venv myenv
```

#### Activate the Virtual Environment :

```
source myenv/bin/activate
```

#### Command to run movies.py :

```
python3 movies.py
```

## POSTGRES CONFIG

```
docker run --name postgres-container --network pgnetwork -e POSTGRES_USER=kel -e POSTGRES_PASSWORD=soen363 -e POSTGRES_DB=movies_db -p 5431:5432 -d postgres

```

## NEO4J CONFIG

```
pip install neo4j
```
