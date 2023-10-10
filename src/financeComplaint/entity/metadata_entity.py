import os
import sys
from pathlib import Path
from financeComplaint.logger import logging
from financeComplaint.exception import CustomException
from financeComplaint.utils import read_yaml_file, write_yaml_file
from collections import namedtuple


DataIngestionMetadataInfo = namedtuple("DataIngestionMetaDataInfo", ['from_date', 'to_date', 'data_file_path'])


class DataIngestionMetadata:
    def __init__(self, metadata_file_path):
        self.metadata_file_path = metadata_file_path        

    @property
    def is_metadata_file_exist(self):
        return os.path.exists(self.metadata_file_path)
    
    def write_metadata_info(self, from_date: str, to_date: str, data_file_path: Path):
        try:
            metadata_info = DataIngestionMetadataInfo(
                from_date=from_date,
                to_date= to_date, 
                data_file_path= data_file_path
            )
            write_yaml_file(path= self.metadata_file_path, data=metadata_info._asdict())
        
        except Exception as e:
            raise CustomException(e, sys)
        

    def get_metadata_info(self) -> DataIngestionMetadataInfo:
        try:
            if not self.is_metadata_file_exist:
                raise Exception(f"WARNING: No metadata file is available at {self.config.metadata_file_path}!")
            
            metadata = read_yaml_file(self.config.metadata_file_path)
            metadata_info = DataIngestionMetadataInfo(**(metadata))
            logging.info("Metadata info is captured!")
            return metadata_info
            
        except Exception as e:
            raise CustomException(e, sys)