from dotenv import load_dotenv
from pyspark.sql import SparkSession
from typing import Optional
from pathlib import Path


class SparkSessionFactory:
    """
    Factory class for creating and managing a singleton SparkSession instance.
    
    Loads environment variables, resolves project root paths,
    configures logging using log4j.properties, and ensures the logs directory exists.
    """

    def __init__(self) -> None:
        """
        Initialize the SparkSessionFactory with resolved paths for the project root,
        log4j configuration, and logs directory.
        """
        load_dotenv()
        self._spark_session: Optional[SparkSession] = None
        self.project_root = Path(__file__).resolve().parents[2]
        self.log4j_config_path = self.project_root / "log4j.properties"
        self.logs_dir = self.project_root / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def get_or_create_spark_session(self) -> SparkSession:
        """
        Return an existing SparkSession or create a new one with custom logging configuration.
        
        Returns:
            SparkSession: A configured Spark session instance.
        """
        if self._spark_session is not None:
            return self._spark_session

        self._spark_session = (
            SparkSession.builder
            .appName("bi_sales_simulator")
            .master("local[3]")
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dlog4j.configuration=file:{self.log4j_config_path} -Dlog.path={self.logs_dir}"
            )
            .getOrCreate()
        )

        self._spark_session._jvm.org.apache.log4j.LogManager.getLogger("bi.application.bi_sales_simulator") \
            .setLevel(self._spark_session._jvm.org.apache.log4j.Level.WARN)

        return self._spark_session
