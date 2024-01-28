from decimal import Decimal
import pandas as pd
from clients.dynamodb_client import DynamoDBClient

# Convert Decimal to int for Pandas compatibility
def convert_decimal(item):
    return {key: int(value) if isinstance(value, Decimal) else value for key, value in item.items()}

def write_dynamodb_items_to_csv(output_path, items):
    df = pd.DataFrame(items)
    df.to_csv(output_path, index=False)

def main():
    # Initialize clients
    dynamodb_client = DynamoDBClient()

    # Fetch the DynamoDB table and get the items
    sample_table = dynamodb_client.fetch_table('kgaban-dynamodb-import-test')
    response = dynamodb_client.scan_table(sample_table)
    items = response['Items']
    converted_items = [convert_decimal(item) for item in items]

    # Write the data from DynamoDB to a CSV file using Pandas
    output_path = "sample-dynamo-data.csv"
    write_dynamodb_items_to_csv(output_path, items=converted_items)

    # Load data into a Pandas DataFrame
    df_pandas = pd.read_csv(output_path)

    ##
    # Reading/Manipulating the data using Pandas
    ##

    # Display the table
    print('TABLE:')
    print(df_pandas)

    # Display the DataFrame info (similar to schema)
    print('INFO:')
    print(df_pandas.info())

    # Display the first two rows
    print('First two rows:')
    print(df_pandas.head(2))

    # Display selected columns
    print('First and Age fields:')
    print(df_pandas[['First', 'Age']])

    # Compute basic statistics for numeric columns
    print('DESCRIBE:')
    print(df_pandas.describe())

    # Add a column (in-place)
    print('Add a column:')
    df_pandas['Age in five years'] = df_pandas['Age'] + 5
    print(df_pandas)

    # Drop columns (in-place)
    print('Drop a column:')
    df_pandas = df_pandas.drop(columns=['Age in five years'])
    print(df_pandas)

    # Rename a column (in-place)
    print('Rename a column:')
    df_pandas = df_pandas.rename(columns={'First': 'First Name'})
    print(df_pandas)


if __name__ == "__main__":
    main()
