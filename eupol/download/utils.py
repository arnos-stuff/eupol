import requests
import tempfile
import warnings
import inspect
import shutil
import pickle
import json
import gzip
import sys
import os

from tqdm import tqdm
from rich.console import Console
from typing import Any, Callable
from pathlib import Path
from functools import wraps
from collections.abc import Iterable

import importlib as imp
import inspect as ins

import importlib.util

rc = Console()
rlog = rc.log

def _hrdict(d: dict):
    fname = "@arg-dict="
    for k,v in d.items():
        fname += f"@key-{k}-@val-{v}"
    fname += "@dict-end"
    return fname

def _hrlist(l: list):
    fname = "@arg-list="
    for i in l:
        fname += f"@val-{i}"
    fname += "@list-end"
    return fname

def human_readable(*args, **kwargs):
    fname = 'hr@args='
    for arg in args:
        if isinstance(arg, dict):
            fname += _hrdict(arg)
        elif isinstance(arg, list):
            fname += _hrlist(arg)
        else:
            fname += "@arg-" + str(arg)
    fname += "@args-end"
    fname += "@kwargs-start"
    fname += _hrdict(kwargs)
    fname += "@kwargs-end"
    return fname

def _jsupp(obj: Any) -> Any:
    if hasattr(obj, 'from_json'):
        return 'from_json'
    elif hasattr(obj, 'read_json'):
        return 'read_json'
    elif hasattr(obj, 'load_json'):
        return 'load_json'
    return None

def _find_io(module: Any, reclvl=0, maxrec=2) -> str:
    # ignore some types of warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)

    # buitins are not what we're looking for
    builtins = [
        *sys.builtin_module_names,
        "os", "enum", "ast", "pathlib",
        "warnings", "inspect", "importlib",
        "abc", "re", "operator", "functools",
        "numbers", "collections", "typing",
        "contextlib", "types", "dataclasses",
        "pickle", "platform", "sysconfig",
        "textwrap", "json", "codecs"
        ]
    
    # rprint(f"🔎 inspecting {module.__name__!r} (reclvl={reclvl})")
    if not ins.ismodule(module):
        raise TypeError(f"expected a module, got {type(module)}")
    if reclvl > maxrec:
        return None
    else:
        # check the filenames for "*io.py" pattern
        # only if not built-in
        if not module.__name__ in builtins:
            modpath = Path(module.__file__).parent
            modpath = modpath.pop() if isinstance(modpath, list) else modpath
            if next(modpath.glob("*io.py"), None):
                # we only want packages, not sort-of builtins
                if module.__package__ != '':
                    return module
                return module        
            elif ".io" in module.__name__:
                return module
            else:
                candidates = ins.getmembers(module, ins.ismodule)
                if not len(candidates):
                    return None
                else:
                    results = []
                    for name, _mod in candidates:
                        if (_smod := _find_io(_mod, reclvl=reclvl+1, maxrec=maxrec)):
                            # no fake builtins
                            results.append(_smod) if _smod.__package__ != '' else None
                    if len(results):
                        # if there is an io module in the candidates, return it
                        ios = [r for r in results if ".io" in r.__name__]
                        if len(ios):
                            rlog(f"Success ✅ found using recuring search {ios[0]}", style="green") if not reclvl else None
                            return ios[0].__name__ if not reclvl else ios[0]
                        # prefer the least nested module
                        results.sort(key=lambda x: x.__name__.count('.'))
                        if not reclvl:
                            rlog(f"Success ✅ found using recuring search for root {module.__name__!r}", style="green")
                            rlog("Out of the following candidates: ", [r.__name__ for r in results], style="blue")
                            # change to [r for r in results] to get all candidates
                            return results[0]
                        return results[0]
                    return None
        
def json_support(obj: Any) -> (str, Any):
    """
    Check if an object has a `to_json` method and a `from_json` (or `read_json`, `load_json`) method.
    This attempts to check withing the parent modules/packages as well.
    If the first 3 layers of submodules withing the root package do not
    have a `to_json` method, then the object is not considered to have
    json support.

    Parameters:
    -----------
    obj: Any

    Returns:
    --------
    str: the name of the module that has the `to_json` and any of the 3 reading methods
    """
    # check the simple case first
    if is_jsonable(obj):
        return json, "loads"
    # bring out the big guns
    mod = imp.import_module(obj.__module__)
    # try on the module first
    if funcname := _jsupp(mod):
        return mod, funcname
    # try on the package root if it exists
    pkgroot = mod.__package__.split('.')
    pkgroot = pkgroot.pop(0) if isinstance(pkgroot, list) else pkgroot
    pkg = imp.import_module(pkgroot)
    if funcname := _jsupp(pkg):
        return pkg, funcname
    else:
        # try to find a .io submodule in the package
        # don't attempt more than a few levels deep
        if modname := _find_io(pkg, maxrec=2):
            mod = imp.import_module(modname)
            if funcname := _jsupp(mod):
                return modname, funcname
        return None

def to_gzip_json(data: Any, path: str):
    jsonfilename = str(path) + ".json.gz" if not path.name.endswith(".json.gz") else str(path)
    os.makedirs(path.parent, exist_ok=True)
    with gzip.open(jsonfilename, 'wt+', encoding='UTF-8') as zipfile:
        json.dump(data, zipfile)

def from_gzip_json(path: str) -> Any:
    jsonfilename = str(path) + ".json.gz" if not path.endswith(".json.gz") else path
    with gzip.open(jsonfilename, 'rt', encoding='UTF-8') as zipfile:
        try:
            return json.load(zipfile)
        except json.decoder.JSONDecodeError as err:
            rlog(f"Error: {err}", style="red")
            rlog(f"File: {jsonfilename}", style="red")
            rlog(zipfile.read(), style="red")
            raise err

def is_jsonable(x: Any) -> bool:
    try:
        json.dumps(x)
        return True
    except (TypeError, OverflowError) as err:
        return False

def check_dependency(name: str):
    if name in sys.modules:
        rlog(f"✅ {name!r} already in sys.modules", style="green")
        return True
    else:
        rlog(f"❌ can't find the {name!r} module", style="red")
        return False

def savetmp(function: Callable):
    @wraps(function)
    def wrapper(*args, **kwargs):
        if not 'directory' in kwargs or not kwargs['directory']:
            tmp = tempfile.gettempdir()
            kwargs['directory'] = str(Path(tmp).joinpath("eupol", function.__name__))
            os.makedirs(kwargs['directory'], exist_ok=True)
            result = function(*args, **kwargs)
        return result
    return wrapper

def funtmpdir(function : Callable, mkdir=False):
    tmp = tempfile.gettempdir()
    tmp = Path(tmp).joinpath("eupol", function.__qualname__)
    if os.path.exists(tmp):
        return str(tmp)
    else:
        if mkdir:
            os.makedirs(tmp, exist_ok=True)
            return str(tmp)
        else:
            return None

def tmpcache(function: Callable) -> Callable:
    directory = funtmpdir(function, mkdir=True)
    
    @wraps(function)
    def wrapper(*args, **kwargs):
        # if class method is called, ignore self
        if '.' in function.__qualname__:
            _args = args[1:]
        else:
            _args = args
        
        # unless explicitly specified, use the temp directory to cache the result
        cache = False if 'cache' in kwargs and not kwargs['cache'] else True
        if cache:
            fvalsname = Path(directory).joinpath(human_readable(*_args, **kwargs))

            if fvalsname.with_suffix(".json.gz").exists():
                rlog(f"> 📁✅ found {fvalsname} in cache", style="green")
                fvalspath = str(fvalsname.with_suffix(".json.gz"))
                stored = from_gzip_json(fvalspath)
                if modname := stored['json_support']["module"]:
                    mod = imp.import_module(modname) if modname != "json" else json
                    if funcname := stored['json_support']["loader"]:
                        jsloader = getattr(mod, funcname)
                        result = jsloader(stored['result'])
                else:
                    result = stored['result']
                return result

            elif fvalsname.with_suffix(".pkl.gz").exists():
                rlog(f"> 📁✅ found {fvalsname} in cache", style="green")
                fvalspath = str(fvalsname.with_suffix(".pkl.gz"))
                with gzip.open(fvalspath, 'rb') as f:
                    return pickle.load(f)
            else:
                rlog(f"> 📁 ⦰ arguments not found in cache", style="purple")
                rlog(f"> ƒ() computing function output ...", style="blue")
                # give class instance in call
                result = function(*args, **kwargs)
                if (jsupp := json_support(result)):
                    result_module, jsloader = jsupp
                if jsupp:
                    if hasattr(result, 'to_json'):
                        serialized_result = result.to_json()
                    elif hasattr(result, 'to_dict'):
                        serialized_result = result.to_dict()
                    elif hasattr(result, 'json'):
                        serialized_result = result.json()
                    else:
                        serialized_result = json.dumps(result)

                data = {
                    "function": function.__name__,
                    'args': _args,
                    'kwargs': kwargs,
                    'result': serialized_result,
                    'json_support': {
                        'module': result_module.__name__ if jsupp else None,
                        'loader': jsloader if jsupp else None
                    }
                }
                rlog(f"> 📁 ⭳⭳ saving result to {fvalsname}", style="blue")
                try:
                    to_gzip_json(data, fvalsname) # if serializable
                except TypeError as err:
                    rlog(f"> 📁 ❌ {fvalsname} is not serializable: {err}", style="red")
                    rlog(data)
                    rlog(f"> 📁 ⭳⭳ saving result to {fvalsname} as pickle (fallback)", style="blue")
                    with gzip.open(fvalsname.with_suffix(".pkl.gz"), 'wb') as f:
                        pickle.dump(result, f)
        return result
    return wrapper

def rmcache(function: Callable):
    directory = funtmpdir(function, mkdir=False)
    if directory:
        shutil.rmtree(directory)
        rlog(f"> ✓ removed cache directory {directory}", style="blue")
        
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

if __name__ == "__main__":
    import pandas, numpy
    # _find_io(module=numpy)
    # _find_io(module=pandas)
    df = pandas.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    jsupp = json_support(obj=df)
    print(jsupp)