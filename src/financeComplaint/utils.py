import os
import sys
import yaml
from financeComplaint.logger import logging
from financeComplaint.exception import CustomException
from box import ConfigBox
from pathlib import Path


def read_yaml_file(path: Path) -> ConfigBox:
    """
    reads yaml file and returns
    Args:
        path (str): path to input
    Raises:
        valueError
    Returns:
        configBox: configBox type
    """
    try:
        with open(path) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logging.info(f'yaml file {yaml_file.name} loaded successfully!')
            return ConfigBox(content)
            
    except Exception as e:
        raise CustomException(e, sys)   


def write_yaml_file(path: Path, data: dict = None) -> None:
    try:
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as yaml_file:
            if data is not None:
                yaml.dump(data, yaml_file)
        logging.info(f'yaml file {yaml_file.name} saved successfully!')
    except Exception as e:
        raise CustomException(e, sys)     



def create_directories(path_to_directories: list, verbos=True):
    """
    create list of directories
    Args:
        path_to_directories (list): list of path of directories
        ignore_log (bool, optional): ignore if multiple dirs is to be created. Defaults to be False
    """
    try:
        for path in path_to_directories:
            os.makedirs(path, exist_ok=True)
            if verbos:
                logging.info(f'Created directory at {path}!')
    except Exception as e:
        raise CustomException(e, sys)


def get_size(path: Path) -> str:
    """
    get size in KB
    Args:
        path (Path): path of the file

    Returns:
        str: size in KB
    """
    try:
        size_in_kb = round(os.path.getsize(path)/1024)
        return f"~ {size_in_kb} KB"
    except Exception as e:
        raise CustomException(e, sys)  