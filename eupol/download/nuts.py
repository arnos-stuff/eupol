import pandas as pd
import geopandas as gpd
import tempfile
import shutil
import re
import os

from pathlib import Path
from typing import Any

from eupol.download.utils import download, funtmpdir

formats = ["geojson", "topojson", "shp", "svg", "pbf"]
geometries = ["RG", "LB", "LN"]
scales = ["01M", "03M", "10M", "20M", "60M"]
years = ["2010", "2013", "2016", "2021"]
projections = ["3035", "4326", "3857"]
levels = ["0", "1", "2", "3"]

baseurl = "https://gisco-services.ec.europa.eu/distribution/v2/nuts"

def metadata(year, directory=None):
    if year not in years:
        raise ValueError(f"Year must be one of {years}")
    
    dfm = pd.read_json("https://gisco-services.ec.europa.eu/distribution/v2/nuts/nuts-2021-files.json")
    dfm.reset_index(inplace=True)
    dfm.rename(columns={"index": "filename"}, inplace=True)
    dfm['raw'] = pd.concat(
        objs=[
        dfm[~dfm.csv.isna()].csv,
        dfm[~dfm.geojson.isna()].geojson,
        dfm[~dfm.topojson.isna()].topojson,
        dfm[~dfm.shp.isna()].shp,
        dfm[~dfm.svg.isna()].svg,
        dfm[~dfm.pbf.isna()].pbf
        ],
        axis=0)
    ptrn = r"(\w+)/NUTS_([\w]{2})_([\w]{2})?_?([\d\w]{3})_([\d]{4})_?([\d]{4})?_?L?E?V?L?_?(\d)?.(\w+)"

    metadata = dfm.apply(
        func=(lambda row: re.findall(ptrn, row.raw).pop() if re.match(ptrn, row.raw) else tuple(8*[''])),
        result_type="expand",
        axis=1,
        ).rename(
            columns={
                0: "format",
                1: "geometry",
                2: "geometry2",
                3: "scale",
                4: "year",
                5: "projection",
                6: "level",
                7: "extension"
                }
            )
    metadata = pd.concat([dfm.drop(columns=["csv", "geojson", "topojson", "shp", "svg", "pbf"]), metadata], axis=1)

    baseurl = "https://gisco-services.ec.europa.eu/distribution/v2/nuts"
    metadata['url'] = (
        baseurl + "/download/" + "ref-nuts-" +
        metadata.year + "-" +
        metadata.scale.str.lower() + "." +
        metadata.extension + ".zip"
    )
    metadata.drop(columns=["raw"], inplace=True)

    if directory is not None:
        Path(directory).mkdir(parents=True, exist_ok=True)
        metadata.to_parquet(Path(directory).joinpath("metadata.parquet"))
    else:
        tmpdir = Path(tempfile.gettempdir()).joinpath("eupol", "metadata")
        tmpdir.mkdir(parents=True, exist_ok=True)
        metadata.to_parquet(tmpdir.joinpath("metadata.parquet"))
    return metadata

def dl(year: str, scale: str, extension: str, directory: str = None):
    """Download NUTS file."""
    if year not in years:
        raise ValueError(f"Year must be one of {years}")
    if scale not in scales:
        raise ValueError(f"Scale must be one of {scales}")
    if extension not in formats:
        raise ValueError(f"Extension must be one of {formats}")
    
    url = (
        baseurl + "/download/" + "ref-nuts-" +
        year + "-" +
        scale.lower() + "." +
        extension + ".zip"
    )
    fname = download(url, directory=directory)

    if fname.endswith(".zip"):
        shutil.unpack_archive(fname, fname.replace(".zip", ""))
    return fname

def path(year: str, fmt: str, geom: str, scale:str, crs: str = "3857", level:str = None) -> str:
    """Return path to NUTS file. If file does not exist, download it first."""
    if year not in years:
        raise ValueError(f"Year must be one of {years}")
    if fmt not in formats:
        raise ValueError(f"Format must be one of {formats}")
    if geom not in geometries:
        raise ValueError(f"Geometry must be one of {geometries}")
    if scale not in scales:
        raise ValueError(f"Scale must be one of {scales}")
    if level and level not in levels:
        raise ValueError(f"Level must be one of {levels}")
    
    tmp = Path(tempfile.gettempdir()).joinpath("eupol", "download")
    geodir = tmp.joinpath(f"ref-nuts-{year}-{scale.lower()}.{fmt}")
    
    if not geodir.exists():
        dl(year, scale, fmt)
    if not level:
        return geodir.joinpath(f"NUTS_{geom}_{scale}_{year}_{crs}.{fmt}")
    else:
        return geodir.joinpath(f"NUTS_{geom}_{scale}_{year}_{crs}_LEVL_{level}.{fmt}")

def as_geodf(year: str, fmt: str, geom: str, scale:str, crs:str = "3857", level:str = None) -> Any:
    p = path(year=year, fmt=fmt, geom=geom, scale=scale, crs=crs, level=level)
    return gpd.read_file(p)

if __name__ == "__main__":
    # metadata = metadata("2021")
    dl("2021", "10M", "geojson")