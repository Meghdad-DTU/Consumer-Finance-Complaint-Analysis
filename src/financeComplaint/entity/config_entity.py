
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: Path
    from_date: str
    to_date: str    
    feature_store_dir: Path 
    downloaded_dir: Path
    failed_downloaded_dir: Path            
    metadata_file_path: Path
    min_start_date: str
    file_name: str    
    datasource_url: str

@dataclass(frozen=True)
class DataValidationConfig:
    root_dir: Path
    feature_store_file_path: Path
    accepted_data_dir: Path
    rejected_data_dir: Path
    file_name: str