from decimal import Decimal
from clients.dynamodb_client import DynamoDBClient
from clients.spark_client import SparkClient
import csv


# Convert Decimal to int for PySpark compatibility
def convert_decimal(item):
    return {key: int(value) if isinstance(value, Decimal) else value for key, value in item.items()}


def write_dynamodb_items_to_csv(output_path, items):
    columns = list(items[0].keys())

    with open(output_path, 'w', newline='') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=columns)
        csv_writer.writeheader()
        csv_writer.writerows(items)


def main():
    # Initialize clients
    dynamodb_client = DynamoDBClient()
    spark_client = SparkClient()

    # Fetch the DynamoDB table and get the items
    dynamodb_table_name = 'kgaban-dynamodb-import-test'
    sample_table = dynamodb_client.fetch_table(dynamodb_table_name)
    response = dynamodb_client.scan_table(sample_table)
    items = response['Items']
    converted_items = [convert_decimal(item) for item in items]

    # Write the data from DynamoDB to a CSV file
    output_path = "sample-dynamo-data.csv"
    write_dynamodb_items_to_csv(output_path, items=converted_items)

    # Load data into the DataFrame
    df_pyspark = spark_client.read_dataframe_from_csv(
        filepath=output_path, inferSchema=True, header=True)

    ##
    #  Reading/Manipulating the data
    ##

    # Display the table
    print('TABLE:')
    df_pyspark.show()

    # Display the schema
    print('SCHEMA:')
    df_pyspark.printSchema()

    # Display the first two rows (returns a List, not a DataFrame)
    print('First two rows:')
    print(df_pyspark.head(2))

    # Display all 'First' and 'Age' fields
    print('First and Age fields:')
    df_pyspark.select(['First', 'Age']).show()

    # Computes basic statistics for numeric and string columns
    print('DESCRIBE:')
    df_pyspark.describe().show()

    # Add a column to a DataFrame (not in-place)
    print('Add a column:')
    df_pyspark = df_pyspark.withColumn(
        'Age in five years', df_pyspark['Age'] + 5)
    df_pyspark.show()

    # Drop columns (not in-place)
    print('Drop a column:')
    df_pyspark = df_pyspark.drop('Age in five years')
    df_pyspark.show()

    # Rename column (not in-place)
    print('Rename a column:')
    df_pyspark = df_pyspark.withColumnRenamed('First', 'First Name')
    df_pyspark.show()

    # Close client sessions
    spark_client.close_session()


if __name__ == "__main__":
    main()
