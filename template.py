import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s:')


project_name = "financeComplaint"

list_of_files = [
    ".github/workflows/main.yaml",
    f"src/{project_name}/__init__.py",
    f"src/{project_name}/components/__init__.py",
    f"src/{project_name}/pipeline/__init__.py",    
    f"src/{project_name}/config/__init__.py",
    f"src/{project_name}/config/configuration.py",
    f"src/{project_name}/entity/__init__.py",
    f"src/{project_name}/entity/config_entity.py",
    f"src/{project_name}/constants/__init__.py",
    f"src/{project_name}/exception.py",
    f"src/{project_name}/logger.py",
    f"src/{project_name}/utils.py",
    f"src/{project_name}/cloud_storage/__init__.py",
    f"src/{project_name}/cloud_storage/s3_syncer.py",
    "config/config.yaml",
    "config/secrets.yaml",
    "config/schema.yaml",
    "app.py",
    "main.py",
    "Dockerfile",
    "params.yaml",
    "requirements.txt",
    "setup.py",
    "README.md",
    "research/trials.ipynb"    
]

for filepath in list_of_files:
    # Path: convert path regardless of operating sytem, e.g., linux is differnt from windows
    filepath = Path(filepath)
    # seperate filename with folder name
    filedir, filename = os.path.split(filepath)

    # 1) Creating directory
    if filedir != '':
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Creating directory {filedir} for the file {filename}")
    
    # 2) Creating files: if the file does not exist or the content is empty
    if (not os.path.exists(filepath)) or (os.path.getsize(filepath)==0):
        with open(filepath, "w") as f:
            pass
            logging.info(f"Creating empty file {filepath}")

    else:
        logging.info(f"{filename} is already exist")



    