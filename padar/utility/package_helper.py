"""Helper functions to get information about the package itself
"""

import pkgutil
import importlib
from . import logger

def list_modules(current_path, package_name):
    current_path = current_path + '/' + package_name
    prefix = current_path.replace('padar/scripts', '').replace('/', '.')
    found_modules = [name for _, name, _ in pkgutil.iter_modules([current_path])]
    module_names = list(map(lambda name: prefix + '.' + name, filter(lambda name: name[0].isupper(), found_modules)))
    module_names = [name[1:] for name in module_names]
    next_modules = list(filter(lambda name: not name[0].isupper(), found_modules))
    for name in next_modules:
        module_names = module_names + list_modules(current_path, name)
    return module_names

def get_doc(module_name):
    try:
        module_obj = importlib.import_module(module_name)
    except ModuleNotFoundError:
        logger.error("Module is not found: " + module_name)
        exit(1)
    return module_obj.__doc__
