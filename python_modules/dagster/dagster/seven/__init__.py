'''Internal py2/3 compatibility library. A little more than six.'''

import sys

from .json import dump, dumps, JSONDecodeError
from .temp_dir import get_system_temp_directory

try:
    FileNotFoundError = FileNotFoundError  # pylint:disable=redefined-builtin
except NameError:
    FileNotFoundError = IOError

if sys.version_info.major >= 3:
    from io import StringIO  # pylint:disable=import-error
else:
    from StringIO import StringIO  # pylint:disable=import-error


# TODO implement a generic import by name -- see https://stackoverflow.com/questions/301134/how-to-import-a-module-given-its-name

# https://stackoverflow.com/a/67692/324449
def import_module_from_path(module_name, path_to_file):
    version = sys.version_info
    if version.major >= 3 and version.minor >= 5:
        import importlib.util

        spec = importlib.util.spec_from_file_location(module_name, path_to_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    elif version.major >= 3 and version.minor >= 3:
        from importlib.machinery import SourceFileLoader

        # pylint:disable=deprecated-method, no-value-for-parameter
        module = SourceFileLoader(module_name, path_to_file).load_module()
    else:
        import imp

        module = imp.load_source(module_name, path_to_file)

    return module


# https://stackoverflow.com/a/437591/324449
def reload_module(module):
    version = sys.version_info
    if version.major >= 3 and version.minor >= 4:
        from importlib import reload as reload_

        return reload_(module)
    elif version.major >= 3:
        from imp import reload as reload_

        return reload_(module)

    return reload(module)  # pylint: disable=undefined-variable
