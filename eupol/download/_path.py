import sys
from pathlib import Path

if sys.version_info >= (3, 9):
    from importlib.resources import files
else:
    from pkg_resources import resource_filename as files

def get_package_dir(pkg: str) -> Path:
    return files(pkg).parent if isinstance(files(pkg), Path)\
        else files(pkg)._paths.pop().parent