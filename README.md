# PySpark, Pandas, AWS DynamoDB, and Microsoft Azure Blob Storage Examples

## Overview

This Python program demonstrates the integration of PySpark and DynamoDB. The project consists of two clients: `DynamoDBClient` for interacting with DynamoDB tables, and `SparkClient` for working with PySpark. The main script, `pyspark-with-dynamo-example.py`, showcases how to read data from a DynamoDB table, convert it, and manipulate it using PySpark. The necessary dependencies are listed in the `requirements.txt` file.

## Project Structure

### 1. `clients/dynamodb_client.py`

   - A DynamoDB client class (`DynamoDBClient`) with methods for initializing the client, fetching a table, and scanning the table.

### 2. `clients/spark_client.py`

   - A Spark client class (`SparkClient`) with methods for initializing a Spark session, reading a DataFrame from a CSV file, and closing the Spark session.

### 3a. `pyspark-with-dynamo-example.py`

   - The main script that utilizes both DynamoDB and PySpark clients.
   - Reads data from a DynamoDB table, converts Decimal types to int for PySpark compatibility, and writes the data to a CSV file.
   - Initializes a Spark session, loads the CSV data into a PySpark DataFrame, and demonstrates various DataFrame manipulations.
   - Closes the Spark session at the end.

### 3b. `pandas-with-dynamo-example.py`

   - This script is (nearly) equivalent to 'pyspark-with-dynamo-example.py' except done with the Pandas library instead.
   - There are some minor differences, for example some operations are in-place in Pandas, whereas in PySpark they are not.

### 4. `azure-with-raw-python.py`

   - This script demonstrates connecting to an Azure Blob Storage using raw Python.
   - The example assumes a csv file exists. This file is then read into a Pandas DataFrame.
   - A .env file must be present to pass credentials/storage info. An example can be found within .env-example.

### 5. `requirements.txt`

   - Lists the required Python packages and their versions for the project.

## Getting Started

1. Clone the repository
2. Install the required dependencies
```bash
pip install -r requirements.txt
```

3. Run the main script
```bash
python pyspark-with-dynamo-example.py
```

## Dependencies

- pyspark==3.5.0
- psutil==5.9.6
- boto3==1.33.10

## Note
Ensure that you have the necessary AWS credentials configured for accessing DynamoDB. Details about the AWS CLI can be found here: https://aws.amazon.com/cli/.

 You will also have to change the table name/data.
***sample-dynamo-data.csv*** is included for reference.

## License
This project is licensed under the MIT License - see the LICENSE file for details.