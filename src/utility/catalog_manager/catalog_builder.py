from pathlib import Path
from dataclasses import dataclass, field
from typing import Set

from utility.path_manager import PathBuilder


@dataclass(frozen=True)
class CatalogSection:
    """
    Represents the root catalog directory and its subdirectories.
    """
    catalog: str = "data"
    catalogs: Set[str] = field(default_factory=lambda: {"operation", "warehouse", "lakehouse"})


class CatalogBuilder:
    """
    Builds the directory structure for the data catalog based on the provided CatalogSection.
    """

    def __init__(self, catalog_section: CatalogSection = CatalogSection()):
        self.catalog_section = catalog_section
        self.path_builder = PathBuilder()
        self.catalog_root = self.path_builder.get_root_directory() / self.catalog_section.catalog
        self.catalog_dirs = [self.catalog_root / name for name in self.catalog_section.catalogs]

    def build_catalog(self):
        """
        Creates the root catalog directory and all specified subdirectories.
        """
        self.catalog_root.mkdir(exist_ok=True)
        for directory in self.catalog_dirs:
            directory.mkdir(exist_ok=True)
