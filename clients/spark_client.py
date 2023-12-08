from pyspark.sql import SparkSession


class SparkClient:
    def __init__(self, app_name='Practice'):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def read_dataframe_from_csv(self, filepath, inferSchema=True, header=True):
        return self.spark.read.csv(filepath, inferSchema=inferSchema, header=header)

    def close_session(self):
        self.spark.stop()
