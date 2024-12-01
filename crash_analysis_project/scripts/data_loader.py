from pyspark.sql import SparkSession

class DataLoader:

    """
        DataLoader class to handle loading datasets using PySpark.

        This class provides methods to load CSV files.

    """
    def __init__(self, spark: SparkSession):

        """
        Initializes the DataLoader object with a given SparkSession.
        Args:
            spark (SparkSession): The SparkSession instance used for loading datasets.
        """
        self.spark = spark

    def load_data(self, file_path: str):

        """
        Loads data from a file
        Args:
            file_path (str): The path to the dataset file.
        Returns:
            DataFrame: The loaded dataset as a PySpark DataFrame.
        """
        return self.spark.read.csv(file_path, header=True, inferSchema=True)
