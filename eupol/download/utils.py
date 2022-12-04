import requests
import tempfile
import sys
import os

from tqdm import tqdm
from pathlib import Path
from functools import wraps


from rich import print
import importlib.util

def check_dependency(name: str):
    if name in sys.modules:
        print(f"✅ {name!r} already in sys.modules", style="green")
        return True
    else:
        print(f"❌ can't find the {name!r} module", style="red")
        return False

def savetmp(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if not 'directory' in kwargs or not kwargs['directory']:
            tmp = tempfile.gettempdir()
            kwargs['directory'] = str(Path(tmp).joinpath("eupol", function.__name__))
            os.makedirs(kwargs['directory'], exist_ok=True)
        result = function(*args, **kwargs)
        return result
    return wrapper

def funtmpdir(function):
    tmp = tempfile.gettempdir()
    tmp = Path(tmp).joinpath("eupol", function.__name__)
    if os.path.exists(directory):
        return str(tmp)
    else:
        return None

@savetmp
def download(url: str, directory: str = None):
    """Download a file from a URL to a directory."""
    fname = url.split("/")[-1]
    fname = str(Path(directory).joinpath(fname))
    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    # Can also replace 'file' with a io.BytesIO object
    with open(fname, 'wb') as file, tqdm(
            desc=fname,
            total=total,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
            ) as bar:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)
    return fname