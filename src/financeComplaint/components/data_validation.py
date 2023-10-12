import os
import sys
from dataclasses import dataclass
from typing import List, Dict
from financeComplaint.logger import logging
from financeComplaint.exception import CustomException
from financeComplaint.entity.schema import FinanceDataSchema
from financeComplaint.config.configuration import DataValidationConfig
from financeComplaint.entity.artifact_entity import DataValidationArtifact
from financeComplaint.config.spark_manager import spark_session
from pyspark.sql.functions import lit, col
from pyspark.sql import DataFrame



@dataclass(frozen=True)
class MissingReport:
    total_row: int
    missing_row: list
    missing_percentage: float

COMPLAINT_TABLE = "complaint"
ERROR_MESSAGE = "error_msg"

class DataValidation:
    def __init__(self, config: DataValidationConfig, table_name: str = COMPLAINT_TABLE, schema=FinanceDataSchema()):
        self.config = config
        self.table_name = table_name
        self.schema = schema

    def read_data(self) -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet(self.config.feature_store_file_path)
            logging.info(f"Data frame is created using file: {self.config.feature_store_file_path}")
            logging.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")
            # Why: only for cutting data size for testing
            # Here, only 10 percent of data will be used
            dataframe, _ = dataframe.randomSplit([0.1, 0.90])
            return dataframe

        except Exception as e:
            raise CustomException(e, sys)
        
    @staticmethod
    def get_missing_report(dataframe: DataFrame) -> Dict[str, MissingReport]:
        try:
            missing_report: Dict[str:MissingReport] = dict()
            logging.info(f"Preparing missing reports for each column")
            number_of_row = dataframe.count()
            for column in dataframe.columns:
                missing_row = dataframe.filter(f"{column} is null").count()
                missing_percentage = (missing_row * 100) / number_of_row
                missing_report[column] = MissingReport(total_row=number_of_row,
                                                        missing_row=missing_row,
                                                        missing_percentage=missing_percentage
                                                        )
            logging.info(f"Missing report prepared: {missing_report}")
            return missing_report
        
        except Exception as e:
            raise CustomException(e, sys)

    def get_unwanted_and_high_missing_value_columns(self, dataframe: DataFrame, threshold: float= 0.3) -> List[str]:
        try:
            missing_report: Dict[str, MissingReport] = self.get_missing_report(dataframe=dataframe)
            unwanted_column: List[str] = self.schema.unwanted_columns
            for column in missing_report:
                if missing_report[column].missing_percentage > (threshold * 100):
                    unwanted_column.append(column)
                    logging.info(f"Missing report {column}: [{missing_report[column]}]")
                unwanted_column = list(set(unwanted_column))
            return unwanted_column
        except Exception as e:
            raise CustomException(e, sys)
        
    def drop_unwanted_columns(self, dataframe: DataFrame) -> DataFrame:
        try:
            unwanted_columns: List = self.get_unwanted_and_high_missing_value_columns(dataframe=dataframe, )
            logging.info(f"Dropping feature: {','.join(unwanted_columns)}")
            unwanted_dataframe: DataFrame = dataframe.select(unwanted_columns)

            unwanted_dataframe = unwanted_dataframe.withColumn(ERROR_MESSAGE, lit("Contains many missing values"))

            rejected_dir = os.path.join(self.config.rejected_data_dir, "missing_data")
            os.makedirs(rejected_dir, exist_ok=True)
            file_path = os.path.join(rejected_dir, self.config.file_name)

            logging.info(f"Writing dropped column into file: [{file_path}]")
            unwanted_dataframe.write.mode("append").parquet(file_path)
            dataframe: DataFrame = dataframe.drop(*unwanted_columns)
            logging.info(f"Remaining number of columns: [{dataframe.columns}]")
            return dataframe
        except Exception as e:
            raise CustomException(e, sys)
        
    @staticmethod
    def get_unique_values_of_each_column(dataframe: DataFrame) -> None:
        try:
            for column in dataframe.columns:
                n_unique: int = dataframe.select(col(column)).distinct().count()
                n_missing: int = dataframe.filter(col(column).isNull()).count()
                missing_percentage: float = (n_missing * 100) / dataframe.count()
                logging.info(f"Column: {column} contains {n_unique} value and missing perc: {missing_percentage} %.")
        except Exception as e:
            raise CustomException(e, sys)
        

    def is_required_columns_exist(self, dataframe: DataFrame):
        try:
            columns = list(filter(lambda x: x in self.schema.required_columns,
                                  dataframe.columns))

            if len(columns) != len(self.schema.required_columns):
                raise Exception(f"Required column missing\n\
                 Expected columns: {self.schema.required_columns}\n\
                 Found columns: {columns}\
                 ")

        except Exception as e:
            raise CustomException(e, sys)
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            logging.info(f"Initiating data preprocessing.")
            dataframe: DataFrame = self.read_data()            

            logging.info(f"Dropping unwanted columns")
            dataframe: DataFrame = self.drop_unwanted_columns(dataframe=dataframe)

            # validation to ensure that all require column available
            self.is_required_columns_exist(dataframe=dataframe)

            logging.info("Saving preprocessed data.")
            print(f"Row: [{dataframe.count()}] Column: [{len(dataframe.columns)}]")
            print(f"Expected Column: {self.schema.required_columns}\nPresent Columns: {dataframe.columns}")

            os.makedirs(self.config.accepted_data_dir, exist_ok=True)
            accepted_file_path = os.path.join(self.config.accepted_data_dir,
                                              self.config.file_name
                                              )
            dataframe.write.parquet(accepted_file_path)

            artifact = DataValidationArtifact(accepted_data_file_path= accepted_file_path,
                                              rejected_data_dir= self.config.rejected_data_dir
                                              )
            logging.info(f"Data validation artifact: [{artifact}]")
            return artifact
        except Exception as e:
            raise CustomException(e, sys)

                 