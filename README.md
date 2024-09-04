# Luke Whittaker code submission for the Lead Data Engineer role for Vita Mojo

I tried my best to stay within the time frame mentioned within the brief. As a result it's not 100% complete, but I
think you are able to at least see where I was going (admittedly with a big more refactoring to be done)


## Repo Layout

- data/ -> I added the test_data file to a directory in the root folder, and read this data in within the Python script
- python/ -> Python ETL script to populate a PostgreSQL DB with the test_data
- sql/ -> SQL queries answering the SQL section
- test/ -> Test folder to test the functionality of functions within the Python script
- .env -> stored the PostgreSQL credentials. Only used for dev work, usually would never push this to GitHub or in production code
- conftest.py -> Config file to tell Pytest to change the port number to our test Docker container when executing SQL queries in PostgreSQL
- docker-compose.yml -> This will create docker containers for test and prod to help separate query executions into PostgreSQL
- requirements.txt -> lists Python packages to install


## Cloning the repo

To clone the repo run this command in your terminal

```shell
git clone 
```


## Setting up the project environment

I used Python3.11 - but it should still work if you use another 3.x version of python

After cloning the GitHub repo, if you then make sure to be in the root folder

Run this command in your terminal: you will need to replace python3.x with either your version or the .exe relative path to the Python
interpreter you are using

```shell
python3.x -m venv .venv
```

This will create a Python 3.x virtual environment and then you can activate the environment with

```shell
source .venv/bin/activate
```

then you can install the packges needed to run the code

```shell
pip intall --no-cache-dir -r requirements.txt
```

The final thing to run will be the docker-compose file, if you run the follwoing it will create and start the Docker containers

```docker
docker-compose up -d
```

Lastly, to run the Python script execute the following in the root folder. Before running you will need to place the test_data.json into the data folder as
the file was too large when committing my code

```shell
python ./python/main.py
```