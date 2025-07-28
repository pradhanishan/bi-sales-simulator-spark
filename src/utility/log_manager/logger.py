from pyspark.sql import SparkSession


class Log4J:
    """
    Wrapper class for Log4J logging in PySpark.
    Creates a namespaced logger based on the Spark application name.
    """

    def __init__(self, spark: SparkSession) -> None:
        log4j = spark._jvm.org.apache.log4j
        root_class = "bi.application"
        app_name = spark.sparkContext.getConf().get("spark.app.name", "DefaultApp")
        self.logger = log4j.LogManager.getLogger(f"{root_class}.{app_name}")

    def warn(self, message: str) -> None:
        self.logger.warn(message)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)
