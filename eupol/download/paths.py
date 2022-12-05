import sys
from pathlib import Path

def module_dir(pkg: str) -> Path:
    return Path(sys.modules.get(pkg).__path__[0])

def data_dir(pkg: str) -> Path:
    return module_dir(pkg).parent.joinpath("assets", "data")

if __name__ == "__main__":
    print(pdir(pkg="eupol"))