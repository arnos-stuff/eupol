import requests
import json
import xmltodict as xtd
import pandas as pd
from typing import Union, Optional, List, Dict, Any, Tuple, Callable

from eupol.download.utils import tmpcache

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
    description = 's:Description'
    code = 's:Code'
    parent = 's:ParentCode'
    level = 's:Level'
    dataflows = 's:Dataflows'
    dataflow = 's:Dataflow'
    annotations = 'c:Annotations'
    annotation = 'c:Annotation'
    codelists = "s:Codelists"
    codelist = "s:Codelist"
    name = 'c:Name'
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
                if sdmxBase.name in k:
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
    def parse(dxml):
        return dxml[sdmxBase.structure][sdmxBase.structures][sdmxBase.dataflows][sdmxBase.dataflow]

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
        cls.flatcodes = cls.flatcodes(_codelist)    
        return cls.flatcodes
    @classmethod
    def df(cls, data: Union[str, dict]):
        """Either a string equal to the scheme id or a dictionary of the parsed xml"""
        _codes = cls.codes(data)
        df = pd.DataFrame.from_records(_codes)
        return df


class Model:
    def __new__(cls, agency_id: str, *args, **kwargs):
        cls.base = sdmxBase
        cls.concept = ConceptScheme(agency_id)
        cls.dataflow = DataFlow
        cls.descendants = Descendants(agency_id)

        cls.agency_id = agency_id
        # store the agency_id in the class
        return cls