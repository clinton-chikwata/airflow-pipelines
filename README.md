# Data Pipelines with Airflow

## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

#### Requirement:
- high grade data pipelines that are dynamic and built from reusable tasks
- monitoring
- allow easy backfills. 
- run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Description
This project introduced me to the core concepts of Apache Airflow. To complete the project, I needed to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## The solution

![dag view](dag.png)

To complete the project, I built four different operators that stage the data, transform the data, and run checks on data quality.

### Stage Operator
The stage operator is able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters specify where in S3 the file is loaded and what is the target table.

Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
With dimension and fact operators, I can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. I also defined a target table that will contain the results of the transformation.

### Data Quality Operator
The final operator is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.