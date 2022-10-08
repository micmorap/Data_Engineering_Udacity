# Udacity Data Engineering Nanodegree - Capstone Project
## By: Michael Andrés Mora P.

--------------------------------

## Introduction and Datasets

The main objective from this first project is apply all the concepts learned along the Data Engineering Nanodegree. I work
with four datasets provided by Udacity. Below, you can see a brief description over each one of datasets:

1. __I94 Immigration Data:__ This dataset was provided by US National Tourism and Trade Office. The aim of this information is create tools to positive growth in travel and tourism along the country and its economy and employment. Moreover, this report provides statistics and insights on KPIs about tourists and non-residents visitants to the US. In the next links you can find general information about this public entity and a dashboard made on Power BI with multiple statistics, indicators per visa types, port of entry, age groups, among others.

2. __World Temperature Data:__ Kaggle is the source of this dataset. The authors of the data (from Berkeley Earth Laboratory), combined 1.6 billion reports around the world. A huge amount of this information was collected by special technicians using mercury and electronic thermometers. The result was global, land and ocean temperaturesand even per countries and states. To a deep explanation about the structure of the data, you can visit the next URL:

3. __US City Demographic Data__ Opendatasoft built this information to all cities in United States with a population greater or equal to 65.000 people. You can find the info in csv, json and excel format and even an API to requests information. You can find in the next URL, a description, visualizations and descriptions about this dataset.

4. __Airport Code Table:__ This data was provided by datahub.io and get the information about the airport code with the international standards (IATA Code, that's mean a three-letter code with their corresponding geographical data). You can find the basic data modeling of this info and the download option (either json or csv extension through multiple programming languages as Python or R)in the next link:

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