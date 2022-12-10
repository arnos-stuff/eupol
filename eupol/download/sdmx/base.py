import rich.progress as rprog
import xmltodict as xtd
import pandas as pd
import requests
import tempfile
import pickle
import json
import os

from typing import Union, Optional, List, Dict, Any, Tuple, Callable
from rich.progress_bar import ProgressBar
from hashlib import sha512 as sha
from rich import print as rprint
from rich.tree import Tree



from eupol.download.utils import tmpcache, rmcache, rc, rlog
from eupol.download.paths import data_dir

def to_snake_case(funcname: str) -> str:
    uppercases = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    predecessor = funcname[0]
    newfuncname = predecessor.lower()
    for letter in funcname[1:]:
        if letter in uppercases:
            if predecessor not in uppercases:
                newfuncname += "_" + letter.lower()
            else:
                newfuncname += letter.lower()
        else:
            newfuncname += letter
        predecessor = letter
    return newfuncname

class annotation:
    title = 'c:AnnotationTitle'
    type_ = 'c:AnnotationType'
    text = 'c:AnnotationText'
    url = 'c:AnnotationURL'
    lang = '@xml:lang'

    def entext(annotext):
        for a in annotext:
            if a[lang] == 'en':
                return a[text]

    def anos(annodict):
       pass # TODO

class sdmxBase:
    structure = 'm:Structure'
    structures = 'm:Structures'
    concepts = 's:Concepts'
    concept = 's:Concept'
    conceptscheme = 's:ConceptScheme'
    code = 's:Code'
    parent = 's:ParentCode'
    level = 's:Level'
    dataflows = 's:Dataflows'
    dataflow = 's:Dataflow'
    categorisations = 's:Categorisations'
    categorisation = 's:Categorisation'
    categoryschemes = "s:CategorySchemes"
    categoryscheme = "s:CategoryScheme"
    category = "s:Category"
    annotations = 'c:Annotations'
    annotation = 'c:Annotation'
    codelists = "s:Codelists"
    codelist = "s:Codelist"
    name = 'c:Name'
    description = 'c:Description'
    lang = '@xml:lang'
    id = '@id'
    version = '@version'
    agency = '@agencyID'
    text = '#text'

    urls = {
    "ESTAT" : "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1",
    "COMP" : "https://webgate.ec.europa.eu/comp/redisstat/api/dissemination/sdmx/2.1",
    "EMPL" : "https://webgate.ec.europa.eu/empl/redisstat/api/dissemination/sdmx/2.1",
    "GROW" : "https://webgate.ec.europa.eu/grow/redisstat/api/dissemination/sdmx/2.1"
    }

    def structural_metadata_url(
        agency_id: str, resource: str,
        all : Optional[bool] = False,
        stubs: Optional[bool] = False,
        details: Optional[bool] = None
        ) -> str:
        if details is None and stubs:
            details = "?detail=allstubs&completestub=true"
        elif details is None:
            details = ""
        else:
            details = f"?detail={details}"
        baseurl = sdmxBase.urls[agency_id]
        _all = "all" if all else ""
        smurl = f"{baseurl}/{resource}/{agency_id}/{_all}{details}"
        return smurl

    _cleantxt = lambda x: to_snake_case(x.replace("s:", "").replace("c:", "").replace("@", "").replace("m:", ""))

    def multilang(data: Union[str, int, float, List, Dict]):
        if isinstance(data, (str, int, float)):
            return data
        elif isinstance(data, list):
            results = list(filter(lambda v: v[sdmxBase.lang] == 'en', data))
            if len(results):
                return results.pop()[sdmxBase.text]
        elif isinstance(data, dict):
            return data[sdmxBase.text]
        else:
            return data

    def recurlang(data: Union[str, int, float, List, Dict]):
        if isinstance(data, (str, int, float)):
            return data
        elif isinstance(data, list):
            return [sdmxBase.recurlang(o) for o in data]
        elif isinstance(data, dict):
            for k in data.keys():
                if sdmxBase.name in k or sdmxBase.description in k:
                    data[k] = sdmxBase.multilang(data[k])
                else:
                    data[k] = sdmxBase.recurlang(data[k])
            return data
        else:
            return data

    def rclean(data: Union[str, int, float, List, Dict]):
        if isinstance(data, (str, int, float)):
            return data
        elif isinstance(data, list):
            return [Descendants.rclean(o) for o in data]
        elif isinstance(data, dict):
            return { sdmxBase._cleantxt(k) : sdmxBase.rclean(v) for k, v in data.items()}



class ConceptScheme(sdmxBase):
    def __new__(cls, agency_id: str = "ESTAT"):
        cls.url = sdmxBase.structural_metadata_url(agency_id, "conceptscheme", stubs=True, all=True)
        return cls
    
    @classmethod
    @tmpcache
    def download(cls, agency_id: str = "ESTAT"):
        cls.response = requests.get(cls.url)
        return xtd.parse(cls.response.text)
    @classmethod
    def parse(cls, data: Union[str, Dict]):
        if isinstance(data, str):
            cls.data = xtd.parse(data)
        elif isinstance(data, dict):
            cls.data = data
            return cls.data[sdmxBase.structure][sdmxBase.structures][sdmxBase.concepts][sdmxBase.conceptscheme]
        elif isinstance(data, list):
            cls.data = data
            return cls.data
        else:
            raise TypeError("data must be a string or a dictionary, not a {}".format(type(data)))
        return cls.data
    @classmethod
    def df(cls, data: Union[str, Dict] = None):
        if data is None:
            cls.data = cls.parse(cls.download())
        elif data.startswith("https://") or data.startswith("http://"):
            cls.data = cls.parse(cls.download(data))
        else:
            cls.data = cls.parse(data)
        cls.asdict = sdmxBase.rclean(sdmxBase.recurlang(cls.data))
        return pd.DataFrame.from_records(cls.asdict)

class DataFlow(sdmxBase):
    def __new__(cls, agency_id: str = "ESTAT"):
        cls.url = sdmxBase.structural_metadata_url(agency_id, "dataflow", stubs=True, all=True)
        return cls
    @classmethod
    def parse(cls, dxml):
        return dxml[sdmxBase.structure][sdmxBase.structures][sdmxBase.dataflows][sdmxBase.dataflow]
    @classmethod
    @tmpcache
    def download(cls, url: str = None):
        if url is None:
            cls.response = requests.get(cls.url)
        else:
            cls.response = requests.get(url)
        cls.data = xtd.parse(cls.response.text)
        return cls.data
    @classmethod
    def flows(cls, data: Union[str, Dict] = None):
        if data is None:
            if not hasattr(cls, 'data'):
                cls.data = cls.parse(cls.download())
            else:
                cls.data = cls.parse(cls.data)
        elif data.startswith("https://") or data.startswith("http://"):
            cls.data = cls.parse(cls.download(data))
        else:
            cls.data = cls.parse(data)

        cls.flows_aslist = sdmxBase.recurlang(cls.data)
        cls.flows_aslist = sdmxBase.rclean(data=cls.flows_aslist)
        return cls.flows_aslist

    @classmethod
    def df(cls, data: Union[str, Dict] = None):
        flows = cls.flows(data)
        return pd.DataFrame.from_records(flows)
class Descendants(ConceptScheme):
    def __new__(cls, agency_id: str) -> str:
        cls.agency_id = agency_id
        cls.url = sdmxBase.structural_metadata_url(cls.agency_id, "dataflow", all=False)
        return cls
    @classmethod
    def download(cls, scheme_id):
        cls.scheme_id = scheme_id
        cls.response = requests.get(cls.url + cls.scheme_id + "?references=descendants")
        cls.data = xtd.parse(cls.response.text)
        return cls.data

    @classmethod
    def flatten(dxml):
        return list(pd.json_normalize(dxml).T.to_dict().values())[0]
    @classmethod
    def flatcodes(cls, _codelist):
        flatcodes = []
        for _code in _codelist:
            if not "code" in _code:
                _code["agency"] = cls.agency_id
                _code["scheme"] = cls.scheme_id if hasattr(cls, "scheme_id") else None
                flatcodes.append(_code)
            else:
                if not isinstance(_code["code"], list):
                    c = _code["code"]
                    c["parent"] = _code["id"]
                    c["agency"] = cls.agency_id
                    c["scheme"] = cls.scheme_id if hasattr(cls, "scheme_id") else None
                
                    flatcodes.append(c)
                else:
                    for c in _code["code"]:
                        c["parent"] = _code["id"]
                        c["agency"] = cls.agency_id
                        c["scheme"] = cls.scheme_id if hasattr(cls, "scheme_id") else None
                        flatcodes.append(c)
        return flatcodes
    @classmethod
    def codes(cls, data: Union[str, dict]):
        """Either a string equal to the scheme id or a dictionary of the parsed xml"""
        if isinstance(data, str):
            dxml = cls.download(data)
        elif isinstance(data, dict):
            dxml = data
        else:
            raise TypeError("data must be a string or a dictionary")

        _codelist = dxml[sdmxBase.structure][sdmxBase.structures][sdmxBase.codelists][sdmxBase.codelist]
        _codelist = sdmxBase.rclean(sdmxBase.recurlang(_codelist))
        cls.codes_aslist = cls.flatcodes(_codelist)    
        return cls.codes_aslist
    @classmethod
    def df(cls, data: Union[str, dict]):
        """Either a string equal to the scheme id or a dictionary of the parsed xml"""
        _codes = cls.codes(data)
        df = pd.DataFrame.from_records(_codes)
        return df

class Categorisation(sdmxBase):
    def __new__(cls, agency_id: str = "ESTAT"):
        cls.url = sdmxBase.structural_metadata_url(agency_id, "categorisation", stubs=False, all=True)
        return cls

    @classmethod
    def flatten(cls, obj):
        if isinstance(obj, (str, int, float)):
            return obj
        elif isinstance(obj, list):
            return [cls.flatten(item) for item in obj]
        elif isinstance(obj, dict):
            flatobj = {}
            for key, val in obj.items():
                if isinstance(val, list):
                    for i, item in enumerate(val):
                        if not isinstance(item, dict):
                            flatobj[key + '.i' + str(i)] = cls.flatten(item)
                        else:
                            for subkey, subval in cls.flatten(item).items():
                                flatobj[key + '.i' + str(i) + '.' + subkey] = subval
                elif not isinstance(val, dict):
                    flatobj[key] = cls.flatten(val)
                else:
                    for subkey, subval in cls.flatten(val).items():
                        flatobj[key + '.' + subkey] = subval
            return flatobj
    
    @classmethod
    def parse(cls, dxml):
        cats = dxml[sdmxBase.structure][sdmxBase.structures][sdmxBase.categorisations]
        cls.data = cats[sdmxBase.categorisation]
        return cls.data
    @classmethod
    @tmpcache
    def download(cls, url: str = None):
        if url is None:
            cls.response = requests.get(cls.url)
        else:
            cls.response = requests.get(url)
        cls.data = xtd.parse(cls.response.text)
        return cls.data
    @classmethod
    def categories(cls, data: Union[str, Dict] = None):
        if data is None:
            if not hasattr(cls, 'data'):
                cls.data = cls.parse(cls.download())
            else:
                cls.data = cls.parse(cls.data)
        elif data.startswith("https://") or data.startswith("http://"):
            cls.data = cls.parse(cls.download(data))
        else:
            cls.data = cls.parse(data)

        cls.categories_aslist = sdmxBase.recurlang(cls.data)
        cls.categories_aslist = sdmxBase.rclean(data=cls.categories_aslist)
        return cls.categories_aslist

    @classmethod
    def df(cls, data: Union[str, Dict] = None, annotations: bool = False):
        cats = cls.categories(data)
        cats = cls.flatten(cats)
        df = pd.DataFrame.from_records(cats)
        if not annotations:
            df = df.drop(columns=[col for col in df.columns if ".annotations" in col])
        return df

class CategoryScheme(sdmxBase):
    def __new__(cls, agency_id: str = "ESTAT"):
        cls.url = sdmxBase.structural_metadata_url(agency_id, "categoryscheme", stubs=False, all=True)
        return cls

    @classmethod
    def flatten(cls, obj):
        if isinstance(obj, (str, int, float)):
            return obj
        elif isinstance(obj, list):
            return [cls.flatten(item) for item in obj]
        elif isinstance(obj, dict):
            flatobj = {}
            for key, val in obj.items():
                if isinstance(val, list):
                    for i, item in enumerate(val):
                        if not isinstance(item, dict):
                            flatobj[key + '.i' + str(i)] = cls.flatten(item)
                        else:
                            for subkey, subval in cls.flatten(item).items():
                                flatobj[key + '.i' + str(i) + '.' + subkey] = subval
                elif not isinstance(val, dict):
                    flatobj[key] = cls.flatten(val)
                else:
                    for subkey, subval in cls.flatten(val).items():
                        flatobj[key + '.' + subkey] = subval
            return flatobj
    
    @classmethod
    def parse(cls, dxml):
        catschemes = dxml[sdmxBase.structure][sdmxBase.structures][sdmxBase.categoryschemes][sdmxBase.categoryscheme]
        cls.data = catschemes
        return cls.data
    @classmethod
    @tmpcache
    def download(cls, url: str = None):
        if url is None:
            cls.response = requests.get(cls.url)
        else:
            cls.response = requests.get(url)
        cls.data = xtd.parse(cls.response.text)
        return cls.data

    @classmethod
    def flatcats(cls, categories: List[Dict], level: int = 0):
        rows = []
        prefix = "level" + str(level) + "." if level > 0 else ""
        for cat in categories:
            row = {}
            if "id" in cat and "name" in cat:
                row[prefix + "id"] = cat["id"]
                row[prefix + "name"] = cat["name"]
            if "category" in cat and isinstance(cat, dict):
                subrows = cls.flatcats(cat["category"], level + 1)
                for subrow in subrows:
                    rows.append({**row, **subrow})
            else:
                rows.append(row)
        return rows

    @classmethod
    def categoryschemes(cls, data: Union[str, Dict] = None):
        if data is None:
            if not hasattr(cls, 'data'):
                cls.data = cls.parse(cls.download())
            else:
                cls.data = cls.parse(data)
        elif data.startswith("https://") or data.startswith("http://"):
            cls.data = cls.parse(cls.download(data))
        else:
            cls.data = cls.parse(data)

        cls.categoryschemes_aslist = sdmxBase.recurlang(cls.data)
        
        cls.categoryschemes_aslist = sdmxBase.rclean(data=cls.categoryschemes_aslist)
        cls.categoryschemes_aslist = cls.flatcats(cls.categoryschemes_aslist)
        return cls.categoryschemes_aslist

    @classmethod
    def df(cls, data: Union[str, Dict] = None, annotations: bool = False):
        catschemes = cls.categoryschemes(data)
        # catschemes = cls.flatten(catschemes)
        df = pd.DataFrame.from_records(catschemes)
        return df

class TableOfContents(sdmxBase):
    def __new__(
        cls,
        dflows: pd.DataFrame,
        dfcategories: pd.DataFrame,
        dfcatschemes: pd.DataFrame
        ):
        
        dfcatschemes['nested_id'] = dfcatschemes.apply(
            func=lambda row : ".".join([
                row[col]
                    for col in dfcatschemes.columns if "id" in str(col) and isinstance(row[col],str)
                    ]),
                    axis=1
                )

        dfcatschemes.columns = ["category_scheme." + str(col) for col in dfcatschemes.columns]

        categories = dfcategories[[
            'source.ref.id', 'target.ref.id',
            'target.ref.maintainable_parent_id',
        ]].drop_duplicates().rename(columns={
            'source.ref.id': 'dataflow.id',
            'target.ref.id': 'category.id',
            'target.ref.maintainable_parent_id': 'category.parent.id',
        })


        flows = dflows[["id", "name", "description"]].rename(columns={
            "id": "dataflow.id",
            "name": "dataflow.name",
            "description": "dataflow.description",
        })


        toc = pd.merge(categories, flows, left_on="dataflow.id", right_on="dataflow.id")
        toc["category.nested_id"] = toc["category.parent.id"] + "." + toc["category.id"]

        toc = pd.merge(
            dfcatschemes,
            toc,
            left_on="category_scheme.nested_id",
            right_on="category.nested_id",
        )

        cls.df = toc
        return cls

    @classmethod
    def from_parquet(cls, path: str):
        cls.toc = pd.read_parquet(path)
        return cls
    
    @classmethod
    def toc_tree(cls, table: Optional[pd.DataFrame] = None):
        """Create a tree of the table of contents at the current state of the class"""
        
        styles = {
            'category_scheme.name': "bold purple",
            'category_scheme.level1.name' : "bold blue",
            'category_scheme.level2.name' : "bold green",
            'category_scheme.level3.name' : "bold yellow",
            'category_scheme.level4.name' : "blue",
            'category_scheme.level5.name' : "green",
            'category_scheme.level6.name' : "yellow",
            'dataflow.name' : "bold red",
        }

        toc = cls.toc if not table else table    
        tocnames = [col for col in toc.columns if "name" in col]
        # capture snapshot of current df through hash
        filename = "toc-snapshot-"
        for col in tocnames:
            filename += f"@col={col}@values{'-'.join(toc[col].dropna().unique())}"
        filename = sha(filename.encode()).hexdigest() + ".pkl"
        filename = os.path.join(tempfile.gettempdir(), "eupol", "sdmx", filename)
        # check if snapshot exists
        if os.path.exists(filename):
            with open(filename, "rb") as f:
                cls.tree = pickle.load(f)
            return cls.tree
        # create tree
        cls.tree = Tree("Table of Contents Topics", style="bold blue")
        nodes = {
            "categories": {
                "visited": {
                    col : [] for col in tocnames
                }
            },
        }
        for idx, row in toc.iterrows():
            rootname = tocnames[0]
            if row[rootname] not in nodes["categories"]["visited"][rootname]:
                nodes["categories"][row[rootname]] = cls.tree.add(row[rootname], style=styles[rootname])
                nodes["categories"]["visited"][rootname].append(row[rootname])
                root = nodes["categories"][row[rootname]]
            else:
                root = nodes["categories"][row[rootname]]
            for col in tocnames[1:]:
                if row[col] is not None:
                    if row[col] not in nodes["categories"]["visited"][col]:
                        nodes["categories"][row[col]] = root.add(row[col], style=styles[col])
                        nodes["categories"]["visited"][col].append(row[col])
                    root = nodes["categories"][row[col]]

        # save tree
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb+") as f:
            pickle.dump(cls.tree, f)
        return cls.tree

class Model:
    def __new__(cls, agency_id: str, *args, **kwargs):
        cls.base = sdmxBase
        cls.concept = ConceptScheme(agency_id)
        cls.dataflow = DataFlow(agency_id)
        cls.descendants = Descendants(agency_id)
        cls.categories = Categorisation(agency_id)
        cls.categoryscheme = CategoryScheme(agency_id)
        cls.ftoc = None

        cls.agency_id = agency_id
        # store the agency_id in the class
        return cls
    
    @classmethod
    def init(cls, directory: Optional[str] = None):
        if directory is None:
            tmp = tempfile.gettempdir()
            directory = os.path.join(tmp, "eupol", "sdmx", cls.agency_id, "metadata", "TOC")
            os.makedirs(directory, exist_ok=True)
        cls.directory = directory
        filename = os.path.join(directory, f"{cls.agency_id}_toc.parquet")
        if os.path.exists(filename):
            cls.ftoc = TableOfContents.from_parquet(filename)
            cls.toc = cls.ftoc.toc
            return cls.toc
        # else download the data
        progress = rprog.Progress(
        rprog.SpinnerColumn(),
        rprog.BarColumn(),
        rprog.TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        rprog.TextColumn("{task.description}"),
        rprog.TimeElapsedColumn(),
        console=rc,
        )

        with progress:
            general = progress.add_task(f"> 🚀🚀 Initializing Metadata for agency {cls.agency_id}", total = 4)
            flowtask = progress.add_task(description=f"[purple] >> ƒ() ⟶  Downloading DataFlow metadata from agency {cls.agency_id}", total=None)
            dflows = cls.dataflow.df()
            progress.update(flowtask, advance=100, description=f"[green] >> ✅ Done ! DataFlow metadata acquired.")
            progress.update(general, advance=1)
            catstask = progress.add_task(description=f"[purple] >> ƒ() ⟶ Downloading Categorisation metadata from agency {cls.agency_id}", total=None)
            dfcategories = cls.categories.df()
            progress.update(catstask, advance=100, description=f"[green] >> ✅ Done ! Categorisation metadata acquired.")
            progress.update(general, advance=1)
            catschemetask = progress.add_task(description=f"[purple] >> ƒ() ⟶  Downloading CategoryScheme metadata from agency {cls.agency_id}", total=None)
            dfcatschemes = cls.categoryscheme.df()
            progress.update(catschemetask, advance=100, description=f"[green] >> ✅ Done ! CategoryScheme metadata acquired.")
            progress.update(general, advance=1)
            toc = progress.add_task(description=f"[purple] >> ƒ() ⟶ Building Table of Contents from agency {cls.agency_id}", total=None)

            cls.ftoc = TableOfContents(
                dflows,
                dfcategories,
                dfcatschemes,
            )
            cls.toc = cls.ftoc.df
            progress.update(toc, advance=100, description=f"[green] >> ✅ Done ! Table of Contents built.")
            progress.update(general, advance=1)
            cls.toc.to_parquet(filename)
            rlog(f"> 📁 ⭳⭳ saving result to {directory} as parquet (for lightning fast IO ⚡⚡)", style="blue")
        return cls.toc

    @classmethod
    def rm_cache(cls):
        rmcache(cls.dataflow.download)
        rmcache(cls.categories.download)
        rmcache(cls.categoryscheme.download)
        rmcache(cls.concept.download)
        rmcache(cls.descendants.download)
        rmcache(cls.init)

if __name__ == '__main__':
    estat = Model("ESTAT")
    # dfcats = estat.categories.df()
    # dfscheme = estat.concept.df()
    # dfflow = estat.dataflow.df()
    # dfcatscheme = estat.categoryscheme.df()
    # estat.rm_cache()
    estat.init()
    print(estat.toc.head(100))