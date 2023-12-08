import boto3


class DynamoDBClient:
    def __init__(self, region_name='us-east-2'):
        self.region_name = region_name
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)

    def fetch_table(self, table_name):
        return self.dynamodb.Table(table_name)

    def scan_table(self, table):
        return table.scan()
