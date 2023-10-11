from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionArtifact:
    feature_store_file_path: Path
    metadata_file_path: Path
    downloaded_dir: Path
