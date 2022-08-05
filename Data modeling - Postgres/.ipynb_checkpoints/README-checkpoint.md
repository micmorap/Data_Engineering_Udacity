
How to run the Python scripts
An explanation of the files in the repository
State and justify your database schema design and ETL pipeline.

# Project 1: Data Modeling
## By: Michael Andrés Mora P.

--------------------------------

## Introduction

The main objective from this first project is apply all the concepts to create and connect a database with PostgreSQL, inser values into tables and build a pipeline with the purpose of automate the input information from the json datasets. The main idea of the database is create an information system about music songs, its name is sparkify!

--------------------
## Analytics goals:

1. Understand the purpose of the project (Business idea)
2. Create the database connection with user, root and host.
3. Create the basic sql statements to drop, create tables and    define variable types.
4. Create the basic sql statements to insert values and invoke from other python scripts.
5. Apply the fact table and relational database concepts with primary and foregins keys.
------------------

## How to run the Python or SQL scripts

Now, We can see the necessary steps:

1. __sql_queries.py__: In this script you must be create the drop and create table statements according the project requests. Also, you should create a list of the statements to invoke in the __create_tables.py__ script.

2. __create_tables.py__: According to the OOP (Object Oriented programming) concepts, when you finished the instructions from the SQL query named above, you should invoke the query lists (to drop, to create tables and to insert values respectively). After, you must be create the define the functions to apply the process to drop tables, create tables and insert the values (previously by default you will find the connection to the PostgreSQL through Psycopg2 library with the credentials).

3. __Open the Terminal__: Through the toolbar you should open a command console or terminal. Then, You must be the next command: **python create_tables.py**. The aim is execute the scripts of the points 1 and 2 previously explained.

4. __test.ipynb__: This jupyter notebook has the purpose of test the quality and avoid programming errors in the scripts describes in the points 1 and 2. Furthermore, if you have problems with the previous steps, this source will help you to determine the mistakes and correct it. When you had finished all the execution of this notebook without problems, you going to the point 5.

5. __etl.ipynb__: When you'll have the architecture and the data structure (in json format) identified, you are going to the instructions described along the notebook. You must be indenfity the json data songs and log_data datasets to then create the ETL process to insert all the data in an automatically form according to the tables parametrized previously in the first step.


6. __etl.py__: This script should be complemented with the script of the point above (Is quite similar to the process applied between the scripts from one and second steps).

7. __Other command in the Terminal__: Finally, You must be the next command: **python etl.py**. The aim is execute the input data process with the __etl__ scripts to insert all the data from the json files.


## Final comments:

In my opinion, I believe that is a good exercise to practice al the ETL processes. If you have any doubt, you can search in the Udacity's forum or ask to a technical mentor. It's essential read the documentation with patience. Thanks for your time!