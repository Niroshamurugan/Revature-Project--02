# Analysis of suicide cases in india

## Project Description

The aim of our project was to analyze the data of suicide cases in India during the period 2001-2012 to findout suicide rates and different reasons due to which people have committed suicide in India. It is very important to understand the reasons to commit suicide and Data Science helps in this regard. Here we analyzed the data by taking various parameters available within the dataset.

## Problem Statement

* What is the percentage of change in suicide cases?
* Which is the age group having the most suicide cases?
* What is the professional profile of the people who committed suicides?
* Which state had the most suicide cases?
* What are the treads in various causes of suicide?

## Technologies Used

* Python
* PySpark
* Spark
* SparkSQL
* Hive
* HDFS
* Git/GitHub  

## Dataset Definition

* state - Shows the name of the state where suicide happened.
* year - Shows the year in which the suicide happened.
* type_code - Shows the general category of the suicide.
* type - Show the specific category of suicide within the general category.
* gender - Shows the gender of the suicide victim.
* age_group - Shows the age group in with the suicide victim belongs to.
* case_count - Show the total cases in that particular category.

## Getting Started
   
* Make sure that the virtualization is enabled for your system from the BIOS.
* Install VMware Workshation Player.
* Download and insatll Hortonworks HDP 2.6.5.
* Get everything up and runninng.
* Connect to the system either through the webshell/OpenSSH/PuTTY.
* Upload all the required files into the local system or clone this repo.

## Usage

To run the hive queries, use the Hive shell

Once the data is loaded into hive, use the `spark-submit` command to run the python program in this repo or use `pyspark` to run each command/operations manually.

# Contributors

* Redon N Roy
* Neha Kumari
* Meenal shree

# License

 This project uses the [MIT](./LICENSE) license.

# References
 [Dataset](https://www.kaggle.com/rajanand/suicides-in-india)
