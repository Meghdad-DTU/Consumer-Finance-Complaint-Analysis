from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionArtifact:
    feature_store_file_path: Path
    downloaded_dir: Path
    metadata_file_path: Path
    

@dataclass(frozen=True)
class DataValidationArtifac:
    accepted_data_file_path: Path
    rejected_data_dir: Path