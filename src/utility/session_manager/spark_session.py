from dotenv import load_dotenv
from pyspark.sql import SparkSession
from typing import Optional
from delta import configure_spark_with_delta_pip

from utility.path_manager import PathBuilder


class SparkSessionFactory:
    """
    Factory class for creating and managing a singleton SparkSession instance.
    """

    def __init__(self) -> None:
        load_dotenv()
        self._spark_session: Optional[SparkSession] = None

        self.path_builder = PathBuilder()
        self.project_root = self.path_builder.get_root_directory()
        self.log4j_config_path = self.project_root / "log4j.properties"
        self.logs_dir = self.path_builder.get_log_directory()
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def get_or_create_spark_session(self) -> SparkSession:
        if self._spark_session is not None:
            return self._spark_session

        builder = (
            SparkSession.builder
            .appName("bi_sales_simulator")
            .master("local[3]")
            .enableHiveSupport()
            .config(
                "spark.driver.extraJavaOptions",
                f"-Dlog4j.configuration=file:{self.log4j_config_path} -Dlog.path={self.logs_dir}"
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

        self._spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

        self._spark_session._jvm.org.apache.log4j.LogManager.getLogger("bi.application.bi_sales_simulator") \
            .setLevel(self._spark_session._jvm.org.apache.log4j.Level.WARN)

        return self._spark_session
