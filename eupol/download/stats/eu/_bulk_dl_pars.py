import tempfile
import eurostat
import os
import json
import pandas as pd
import rich.progress as rprog

from eupol.download.stats.eu.tfilter import toc
from eupol.download.utils import rlog, rc
from eupol.download.stats.eu.tprogress import TimeBarColumn, TaskSecondsColumn

"""Original code from the author of the eurostat package
"""

# def get_pars(code):
#     """
#     Download the pars to filter the Eurostat dataset with given code.
#     Return a list.
#     """
#     assert type(code) is str, "Error: 'code' must be a string."
    
#     __, __, dims = __get_dims_info__(code)
#     return [d[1] for d in dims]


# def __get_dims_info__(code):
#     # dims = [(position, codelist_name, dimension_ID), ...]
#     agencyId, provider, dsd_code = __get_info__(code)
#     dsd_url = __Uri__.BASE_URL[provider] + "datastructure/" + agencyId + "/" + dsd_code + "/latest"
#     resp = __get_resp__(dsd_url)
#     root = __get_xml_root__(resp)
#     dims = [(dim.get("position"), dim.get("id"), dim.find(__Uri__.ref_path).get("id"))
#              for dim in root.findall(__Uri__.dim_path)]
#     return [agencyId, provider, dims]

# def __get_info__(code):
#     agency_by_provider = [("EUROSTAT", "ESTAT"),
#                           ("COMEXT", "ESTAT"),
#                           ("COMP", "COMP"),
#                           ("EMPL", "EMPL"),
#                           ("GROW", "GROW"),
#                           ]
#     found = False
#     i = 0
#     while (not found) and (i <= len(agency_by_provider)):
#         try:
#             url = __Uri__.BASE_URL[agency_by_provider[i][0]] +\
#                     "dataflow/" +\
#                     agency_by_provider[i][1] +\
#                     "/" +\
#                     code
#             resp = __get_resp__(url, is_raise=False)
#             root = __get_xml_root__(resp)
#             agencyId = agency_by_provider[i][1]
#             provider = agency_by_provider[i][0]
#             found = True
#         except:
#             pass
#         i += 1
#     if not found:
#         print("Dataset not found: " + code)
#         raise ValueError
#     else:
#         dsd_code = root.find(__Uri__.dsd_path).get("id")
#         return [agencyId, provider, dsd_code]



"""My attempt to make it faster using tornado and multiprocessing
"""

from tornado import ioloop, gen
from tornado.httpclient import AsyncHTTPClient, HTTPError
import xml.etree.ElementTree as ET
import gzip as gz

baseurl = {
    "EUROSTAT": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/",
    "COMEXT": "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/",
    "COMP": "https://webgate.ec.europa.eu/comp/redisstat/api/dissemination/sdmx/2.1/",
    "EMPL": "https://webgate.ec.europa.eu/empl/redisstat/api/dissemination/sdmx/2.1/",
    "GROW": "https://webgate.ec.europa.eu/grow/redisstat/api/dissemination/sdmx/2.1/",
    }

XMLSNS_M = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}"
XMLSNS_S = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}"
XMLSNS_C = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}"

dim_path = \
    XMLSNS_M + "Structures/" +\
    XMLSNS_S + "DataStructures/" +\
    XMLSNS_S + "DataStructure/" +\
    XMLSNS_S + "DataStructureComponents/" +\
    XMLSNS_S + "DimensionList/" +\
    XMLSNS_S + "Dimension"
dsd_path = \
    XMLSNS_M + "Structures/" +\
    XMLSNS_S + "Dataflows/" +\
    XMLSNS_S + "Dataflow/" +\
    XMLSNS_S + "Structure/Ref"
ref_path = \
    XMLSNS_S + "LocalRepresentation/" +\
    XMLSNS_S + "Enumeration/Ref"

provider_agency = [
        ("EUROSTAT", "ESTAT"),
        ("COMEXT", "ESTAT"),
        ("COMP", "COMP"),
        ("EMPL", "EMPL"),
        ("GROW", "GROW"),
        ]

def _dsdurl(provider, agencyId, dsd_code):
    return baseurl[provider] + "datastructure/" + agencyId + "/" + dsd_code + "/latest"

def _dataflowurl(provider, agencyId, dsd_code):
    return baseurl[provider] + "dataflow/" + agencyId + "/" + dsd_code

def _dataflows(code):

    flows = []

    for provider, agency in provider_agency:
        flows += [ _dataflowurl(provider, agency, code) ]
    return flows

def _flatten(l: list) -> list:
    return [item for sublist in l for item in sublist ]

@gen.coroutine
def fetch_and_handle(urls, progress=None, task=None):
    """Fetches the urls and handles/processes the response"""

    http_client = AsyncHTTPClient()
    waiter = gen.WaitIterator(*[http_client.fetch(url) for url in urls])

    errs = []
    responses = []
    
    while not waiter.done():
        try:
            response = yield waiter.next()
            responses += [response]
        except Exception as e:
            errs += [e]
        
    rlog(f"Got {len(responses)} hits after {len(errs)} failed attemps ({len(responses)}/{len(errs)+len(responses)})", style="green")

    if not len(responses):
        rlog("No responses !", style="red")
        map(lambda msg: rlog(msg, style="bold red"), errs) # print the errors
        raise HTTPError(404, "No responses !")

    return responses


def info(response, decompress=True):
    for provider, agency in provider_agency:
        if response.effective_url.startswith(baseurl[provider]):
            if decompress:
                root = ET.fromstring(gz.decompress(
                        response.body
                        ).decode("utf-8")
                        )
                dsd_code = root.find(dsd_path).get("id")
            else:
                root = ET.fromstring(
                    response.body.decode("utf-8")
                    )
                dsd_code = root.find(dsd_path).get("id")
            return [provider, agency, dsd_code]
    
    raise ValueError("No provider found for url: " + response.effective_url)

def _dims_url(response):
    provider, agency, dsd_code = info(response)
    return _dsdurl(provider, agency, dsd_code)

def _bulk_dims_url(responses):
    return [ _dims_url(response) for response in responses ]

def dims(response, decompress=True):
    """Returns the dimensions of the root"""
    if decompress:
        root = ET.fromstring(gz.decompress(
                response.body
                ).decode("utf-8")
                )
    else:
        root = ET.fromstring(
            response.body.decode("utf-8")
            )
    
    return [ (dim.get("position"), dim.get("id"), dim.find(ref_path).get("id")) for dim in root.findall(dim_path) ]

# time estimates: 100 codes -> 7s, 500 codes -> 21s thus 7500 codes -> 315s
# suppose variance is less than 2 for 200 => 2/200 = 0.01 per unit for a mean of 21/500 = 0.042s per unit
if __name__ == '__main__':
    loop = ioloop.IOLoop.current()
    codes = toc().code.unique()
    rlog(f"Downloading 100 out of {len(codes)}", style="bold green")
    codes = codes[:100]
    progress = rprog.Progress(
        rprog.SpinnerColumn(),
        TimeBarColumn(total_time=len(codes)*0.062),
        TaskSecondsColumn(total_time=len(codes)*0.062),
        rprog.TimeElapsedColumn(),
        rprog.TextColumn(f"Estimated time: {len(codes)*0.062:0.3f} ± {3*(len(codes)*0.01)**(0.5):0.3f}"),
    )
    urls = _flatten([ _dataflows(code) for code in codes ])
    task = progress.add_task(
        description=f"[green]Downloading dsd codes from {len(urls)} urls"
    )
    with progress:
        responses = loop.run_sync(lambda: fetch_and_handle(urls, progress=progress, task=task), timeout=60)
    dims_urls = _bulk_dims_url(responses)

    progress.remove_task(task)
    task = progress.add_task(
        description=f"[green]Downloading dimensions from {len(dims_urls)} urls"
    )
    with progress:
        dims_responses = loop.run_sync(lambda: fetch_and_handle(dims_urls, progress=progress, task=task), timeout=60)
        alldims = [ [*info(ires), *d] for ires, dimres in zip(responses, dims_responses) for d in dims(dimres) ]
    df = pd.DataFrame(alldims, columns=["provider", "agency", "dsd_code", "dim:position", "dim:id", "dim:ref"])
    rlog(df)
    # dfinfo = pd.DataFrame([ info(response) for response in responses ], columns=["provider", "agency", "dsd_code"])
    # rlog(dfinfo)
    # rootinfo = maproots(responses)
    # info = mapinfo(rootinfo)
    # rlog(info)
    # dims = [ dims(root) for root in roots ]
    # rlog(dims)