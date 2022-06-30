
import os
from pathlib import Path

# ----------------------------------------------------- #
# * Generate Path Name
# ----------------------------------------------------- #
def generate_path(base_path:str, str_date:str, variable:any, mode=3):
    """Generate path name, no file extensions added yet.

    Parameters
    ----------
    base_path: str, start path prefix
    str_date: str, date
    variable: str, climate variable
    mode: int
        1: path
        2: filename
        3: path+filename
        4: {"path": path, "fn": fn, "all": path+fn}
    """

    # Check for variale type
    if type(variable) is list: variable=variable[0]
    path = f"{base_path}/{variable}/{str_date}"
    fn = f"{str_date}_{variable}"

    # Return path name generated based on variable
    # path only
    if mode == 1:
        return base_path
    # filename only
    elif mode ==2:
        return fn
    # path & filename
    elif mode == 3:
        return f"{path}/{fn}"
    elif mode == 4:
        return {"path": path, "fn": fn, "all": f"{path}/{fn}"}

# ----------------------------------------------------- #
# * Create directory
# ----------------------------------------------------- #
def mkdir(path:str, force:bool=False):
    if not os.path.isdir(path) or force == True:
        os.makedirs(path, exist_ok=True)


# ----------------------------------------------------- #
# * Locate files
# ----------------------------------------------------- #
def locate(filename:str, mode=0) -> list:
    """
    Locate a file locally
    Returns a list of paths

    filename: str, search keyword
    mode: int   
        0: absolute path
        1: name

    """
    
    paths = []
    search_path = f"/opt/airflow/data"
    for path in Path(search_path).rglob('*.grib'):
        if mode == 0:
            get = str(path.absolute()) 
        elif mode == 1:
            get = path.name
        paths.append(get)
    
    return paths
    
    # other method:
    # for root, dirnames, filenames in os.walk('src'):
    #     for filename in fnmatch.filter(filenames, '*.c'):
    #         print(os.path.join(root, filename))
    
def movedir(src_dir, dest_dir) -> bool:
    os.system(f"cp -r -p {src_dir} {dest_dir}")
    
    return True