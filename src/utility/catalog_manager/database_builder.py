from dataclasses import dataclass, field
from typing import Set, Dict
from pyspark.sql import SparkSession
from pathlib import Path

from utility.path_manager import PathBuilder


@dataclass(frozen=True)
class DatabaseSection:
    """
    Logical grouping of databases used in the project.
    """
    operation: Set[str] = field(default_factory=lambda: {"master", "transaction"})
    warehouse: Set[str] = field(default_factory=lambda: {"data_stage", "temp", "data_mart"})
    lakehouse: Set[str] = field(default_factory=lambda: {"bronze", "silver", "gold"})


class DatabaseBuilder:
    """
    Builds required Spark databases if they do not already exist.
    """

    def __init__(self, spark: SparkSession, dbs: DatabaseSection = DatabaseSection()) -> None:
        self.spark = spark
        self.dbs = dbs
        self.path_builder = PathBuilder()
        self.catalog_root = self.path_builder.get_data_directory()

    def build_databases(self) -> None:
        """
        Creates all databases (operation, warehouse, lakehouse) if they don't exist.
        """
        database_groups: Dict[str, Set[str]] = {
            "operation": self.dbs.operation,
            "warehouse": self.dbs.warehouse,
            "lakehouse": self.dbs.lakehouse,
        }

        for group_name, db_names in database_groups.items():
            for db in db_names:
                if not self.spark.catalog.databaseExists(db):
                    db_path = self.catalog_root / group_name / db
                    self.spark.sql(f"CREATE DATABASE {db} LOCATION '{str(db_path)}'")
                    print(f"✅ Created database: {db} at {db_path}")
