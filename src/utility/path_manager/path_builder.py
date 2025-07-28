from pathlib import Path


class PathBuilder:
    """
    Utility class to resolve the project root and construct common directory paths.
    This class assumes that 'pyproject.toml' exists at the root of the project.
    """

    def __init__(self):
        """
        Initializes the PathBuilder by resolving the project root.
        """
        self.root = self._resolve_project_root()

    def _resolve_project_root(self) -> Path:
        """
        Traverses up the directory structure to find 'pyproject.toml' and returns the root path.

        Returns:
            Path: Path object pointing to the root of the project.

        Raises:
            RuntimeError: If 'pyproject.toml' is not found in any parent directories.
        """
        current_path = Path(__file__).resolve()
        for parent in current_path.parents:
            if (parent / "pyproject.toml").exists():
                return parent
        raise RuntimeError("Project root not found. 'pyproject.toml' is required at the root.")

    def get_root_directory(self) -> Path:
        """
        Returns the resolved project root directory.

        Returns:
            Path: Root directory of the project.
        """
        return self.root

    def get_data_directory(self) -> Path:
        """
        Returns the default data directory path (project_root/data).

        Returns:
            Path: Path to the data directory.
        """
        return self.root / "data"

    def get_log_directory(self) -> Path:
        """
        Returns the default log directory path (project_root/logs).

        Returns:
            Path: Path to the logs directory.
        """
        return self.root / "logs"

    def get_src_directory(self) -> Path:
        """
        Returns the default source directory path (project_root/src).

        Returns:
            Path: Path to the src directory.
        """
        return self.root / "src"
