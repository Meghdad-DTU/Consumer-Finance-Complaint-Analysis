from setuptools import find_packages, setup
from typing import List


HYPEN_E_DOT = '-e .'
def get_requirments(file_path:str)->List[str]:
    '''
    This function will retun the list of requirments
    '''
    requirements = []
    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n","") for req in requirements]
        
        if HYPEN_E_DOT in requirements:
            requirements.remove(HYPEN_E_DOT)
            
    return requirements    

__version__ = '0.0.1'
REPO_NAME = 'Consumer-Finance-Complaint-Analysis'
AUTHOR_USER_NAME = 'Meghdad-DTU'
SRC_REPO = 'financeComplaint'
AUTHOR_EMAIL = 'mehdizadeh.iust@gmail.com'



setup(
    name= SRC_REPO,
    version = __version__,
    author = 'Meghdad',
    author_email = AUTHOR_EMAIL,
    url=f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}",
    package_dir={"":"src"},
    packages = find_packages(where='src'),
    install_requires = get_requirments('requirements.txt')
    )