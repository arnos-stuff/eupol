import rich.progress as rprog
from tornado import ioloop, gen
from tornado.httpclient import AsyncHTTPClient, HTTPError
from pandasdmx.reader.sdmxml import Reader
from typing import List, Dict, Tuple, Callable, Iterable, Union, Optional
import gzip as gz

from eupol.download.utils import rlog, rc
from eupol.download.sdmx.tprogress import TimeBarColumn

sdmxl = Reader()


@gen.coroutine
def fetch_and_handle(urls):
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

def nested_queries_download(
    callbacks:Union[List[Callable], Iterable[Callable]],
    timeout:Optional[int]=60,
    history:Optional[bool]=False,
    ):
    """
    Supposes the callbacks are ordered in a way that the first callback
    returns a list of urls to be downloaded and the second callback
    takes the responses of the first callback and returns another list of urls, etc.

    The last callback is expected to process the ultimate responses.
    If not, the last callback is expected to return a list of urls, which will be
    returned by the function.
    """
    if isinstance(callbacks, list):
        nb_callbacks = len(callbacks)
        callbacks = iter(callbacks)
    elif isinstance(callbacks, Iterable):
        nb_callbacks = callbacks.__length_hint__()
    if not isinstance(callbacks, Iterable):
        raise TypeError(f"callbacks must be an iterable, not a single callback or {type(callbacks)}")

    http_client = AsyncHTTPClient()
    progress = rprog.Progress(
        rprog.SpinnerColumn(),
        rprog.BarColumn(),
        rprog.TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        rprog.TextColumn("[progress.percentage]{task.completed} / {task.total}"),
        rprog.TimeElapsedColumn("{task.description}"),
    )
    loop = ioloop.IOLoop.current()
    outer_loop = progress.add_task(description=f"[purple] > Running {nb_callbacks} callbacks to get nested url responses.", total=nb_callbacks)
    with progress:
        call = next(callbacks)
        ini = progress.add_task(description=f"[purple] > ƒ() ⟶ urls from first callback {call.__name__}", total=None)
        urls = call()
        progress.update(ini, description=f"[green] > ✅ Done: urls from first callback ⟶ {len(urls)} urls")
        progress.remove_task(ini)

        nested_responses = []
        
        for i, call in enumerate(callbacks):
            waiter = gen.WaitIterator(*[http_client.fetch(url) for url in urls])
            task = progress.add_task(description=f"[purple] >> ⤓ ⤓ get() call on {len(urls)}", total=None)
            
            responses = loop.run_sync(lambda: fetch_and_handle(urls), timeout=timeout)
            nested_responses += [responses]

            progress.update(task, description=f"[green] > ✅ Done: urls from callback ⟶ ({len(responses)}/{len(urls)}) responses")
            progress.update(outer_loop, advance=1)
            progress.update(outer_loop, description=f"[purple] > Running {nb_callbacks-i-1} callbacks to get nested url responses.")
            progress.update(task, description=f"[purple] > ƒ() ⟶ urls from callback {call.__name__} ({i+1}/{nb_callbacks})")
            urls = call(responses)
            progress.update(task, description=f"[green] > ✅ Done: urls from callback ⟶ {len(urls)} urls")
            progress.remove_task(task)
    return nested_responses if history else urls # supposes the last callback processes the responses

if __name__ == '__main__':
    callbacks = [
        lambda: ["https://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/ALL"],
    ]