from datetime import datetime
import os
from financeComplaint.constants import *
from financeComplaint.utils import read_yaml_file, create_directories
from financeComplaint.entity.metadata_entity import DataIngestionMetadata
from financeComplaint.entity.config_entity import DataIngestionConfig


class ConfigurationManager:
    def __init__(self,
                 config_filepath=CONFIG_FILE_PATH,                 
                 params_filepath=PARAMS_FILE_PATH,
                 saved_modelpath=SAVED_MODEL_PATH,
                 ):
       
        self.config = read_yaml_file(config_filepath)
        self.params = read_yaml_file(params_filepath)
        self.saved_modelpath = saved_modelpath
        
        create_directories([self.config.artifacts_root])
        self.timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S') 
        
    
    def get_data_ingestion_config(self, from_date: str=None, to_date: str=None) -> DataIngestionConfig:
        """
        from date can not be less than min start date

        if to_date is not provided automatically current date will become to date

        """

        config = self.config.data_ingestion
        SUB_ROOT_DIR = os.path.join(config.ROOT_DIR, self.timestamp)
        DOWNLOADED_DIR = os.path.join(SUB_ROOT_DIR,'downloaded_files')
        FAILED_DOWNLOADED_DIR = os.path.join(SUB_ROOT_DIR,'failed_downloaded_files')
        

        create_directories([config.ROOT_DIR, 
                            config.FEATURE_STORE_DIR,
                            DOWNLOADED_DIR, 
                            FAILED_DOWNLOADED_DIR, 
                            ])
    
        def validate(date_text):
            try:
                if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
                    raise ValueError
                return True
            except ValueError:
                return False
        
          
        min_start_date= config.MIN_START_DATE 
        if  not validate(min_start_date):
            raise Exception(f"WARNING: Minimum start date: {min_start_date} does not have correct format!")    
        
        if from_date is None:
            from_date = min_start_date 
        else:
            if not validate(from_date):
                raise Exception(f"WARNING: From date: {from_date} does not have correct format!")
        
        if from_date < min_start_date:
            from_date = min_start_date
        
        if to_date is None:
            to_date = datetime.now().strftime("%Y-%m-%d")  
        
        else:
            if not validate(to_date):
                raise Exception(f"WARNING: To date : {to_date} does not have correct format!") 

        data_ingestion_metadata= DataIngestionMetadata(config.METADATA_FILE_PATH)

        if data_ingestion_metadata.is_metadata_file_exist:
            metadata_info= data_ingestion_metadata.get_metadata_info()
            from_date = metadata_info.to_date
        
        
        data_ingestion_config = DataIngestionConfig(
            root_dir = config.ROOT_DIR,
            from_date= from_date,
            to_date= to_date,
            feature_store_dir= config.FEATURE_STORE_DIR,   
            downloaded_dir = DOWNLOADED_DIR,
            failed_downloaded_dir= FAILED_DOWNLOADED_DIR,                     
            metadata_file_path= config.METADATA_FILE_PATH,
            min_start_date= config.MIN_START_DATE,  
            file_name= config.FILE_NAME,         
            datasource_url= config.DATASOURCE_URL, 

        )


        return data_ingestion_config