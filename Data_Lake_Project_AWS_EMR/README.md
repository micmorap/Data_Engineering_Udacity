# Project: Data Lakes
## By: Michael Andrés Mora P.

--------------------------------

## 1. Introduction

The main objective from thisproject is apply all the concepts to create a datalake from a data warehouse using several tools how Spark, PySpark and AWS tools how S3, EMR (Elastic Map Reduce) and having into account the start schema as well. The data is stored in S3 Buckets, then We should process this data in json format, stablish the corresponding partitions and create the parquet files for storage again in a S3 Bucket.All of this, inside a cluster inside Amazon Web Services EMR. Finally, the records will be insert into parquet files and build a pipeline with the purpose of automate the input information from these ones. The main idea of the datalake is create an information semi and unstructured to Sparkify!

--------------------
## 2. Analytics goals:

A. Understand the purpose of the project (Business idea)
B. Create the corresponding fact, dimension and staging tables throught the class SQL sintaxis with the PySpark support.
C. Create the basic  statements to invoke, create and enrich the tables and define variable types.
D. Run the etl python script step by step to complete the process.

------------------

## 3. How to run the Python scripts:

Now, We can see the necessary steps:

1. __dl.cfg__: Put the credentials for access to AWS (S3 and EMR).

2. __Run the next command in the Terminal__: Finally, You must be the next command: **python etl.py**. The aim is execute the input data process with the __etl.py__ script to insert all the data from the parquet files and then send the info towards S3 Bucket.


## Final comments:

As to sum up, is essential have a previous and minimum understanding of PySpark and be careful with the costs of AWS tools (you can have problems with the costs!). Moreover, the difference between datalakes and datawarehousing is basically the power and performance to storage structured, semi-structured and unstructured info (the datawarehouse is to storage data structured). On the other hand, you must need understand the basic architecture to the Spark on cloud technologies. If you have millions of records in your data, probably you need it and understand how run the pipelines and other processes in an efficient way, if is not the case, you can use the normal tools how pandas, jupyter, SQL engines in your local machine. Finally, is very interesting how to know work the parallelism and distributed computing. 