import sys
from financeComplaint.exception import CustomException
from financeComplaint.logger import logging
from financeComplaint.config.configuration import ConfigurationManager
from financeComplaint.components.data_validation import DataValidation


STAGE_NAME = "Data Validation Stage"

class DataValidationPipeline:
    def __init__(self):
        pass

    def main(self):
        config = ConfigurationManager()
        data_validation_config = config.get_data_validation_config()
        data_validation = DataValidation(config=data_validation_config)
        data_validation.initiate_data_validation()

if __name__ == '__main__':
    try:        
        logging.info(f'>>>>>>> stage {STAGE_NAME} started <<<<<<<<')
        obj = DataValidationPipeline()
        obj.main()
        logging.info(f'>>>>>>> stage {STAGE_NAME} completed <<<<<<<<')
    
    except Exception as e:
        raise CustomException(e, sys)