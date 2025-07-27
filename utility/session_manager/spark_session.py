from dotenv import load_dotenv
from pyspark.sql import SparkSession
from typing import Optional


class SparkSessionFactory:
    """
    Factory class to create and manage a singleton SparkSession instance.

    Loads environment variables using dotenv, and lazily initializes
    a SparkSession with a predefined configuration.
    """

    def __init__(self) -> None:
        """
        Initialize the SparkSessionFactory and load environment variables.
        """
        load_dotenv()
        self._spark_session: Optional[SparkSession] = None

    def get_or_create_spark_session(self) -> SparkSession:
        """
        Retrieve an existing SparkSession or create a new one if not already initialized.

        Returns:
            SparkSession: A singleton SparkSession instance.
        """
        if self._spark_session is not None:
            return self._spark_session

        self._spark_session = (
            SparkSession.builder
            .appName("Bi Sales Simulator")
            .master("local[3]")
            .getOrCreate()
        )
        return self._spark_session
