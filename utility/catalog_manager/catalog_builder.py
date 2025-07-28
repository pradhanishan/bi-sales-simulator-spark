from pathlib import Path
from dataclasses import dataclass, field
from typing import Set

@dataclass(frozen=True)
class CatalogSection:
    """
    Represents the root catalog directory and its subdirectories.

    Attributes:
        catalog (str): Name of the top-level catalog directory.
        catalogs (Set[str]): Set of catalog subdirectories to create under the root.
    """
    catalog: str = "catalog"
    catalogs: Set[str] = field(default_factory=lambda: {"operation", "warehouse", "lakehouse"})

class CatalogBuilder:
    """
    Builds the directory structure for the data catalog based on the provided CatalogSection.

    Attributes:
        cs (CatalogSection): Catalog section configuration.
        catalog_path (Path): Full path to the root catalog directory.
        catalogs (List[Path]): List of full paths for each subdirectory under the catalog.
    """

    def __init__(self, catalog_section: CatalogSection = CatalogSection()):
        """
        Initializes the CatalogBuilder.

        Args:
            catalog_section (CatalogSection, optional): Custom catalog configuration. Defaults to CatalogSection().
        """
        self.cs = catalog_section
        self.catalog_path = Path(".").resolve().parents[1] / self.cs.catalog
        self.catalogs = [self.catalog_path / c for c in self.cs.catalogs] 
    
    def build_catalog(self):
        """
        Creates the root catalog directory and all specified subdirectories.
        If the directories already exist, they are left unchanged.
        """
        self.catalog_path.mkdir(exist_ok=True)
        for c in self.catalogs:
            c.mkdir(exist_ok=True)
