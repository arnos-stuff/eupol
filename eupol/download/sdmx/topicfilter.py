import os
import pickle
import tempfile
import pandas as pd
from typing import List, Union, Dict, Any, Optional
from copy import deepcopy
from difflib import SequenceMatcher
from rich.console import RenderResult
from rich.tree import Tree
from rich import print as rprint
from hashlib import md5


from eupol.download.utils import rlog, rc, _hrdict
from eupol.download.paths import data_dir
from eupol.download.text import tokenize, tokens, parse
from eupol.download.sdmx.base import Model

def _search_tmp_hash(sentence: Union[List[str], str], **kwargs):
    if isinstance(sentence, list):
        sentence = "+".join([parse(s).replace(" ", "-") for s in sentence])
    else:
        sentence = parse(sentence).replace(" ", "-")
    filename = f"@search={sentence}"+_hrdict(kwargs)
    if len(filename) <= 20:
        return filename + ".parquet"
    else:
        filename = "df-store-@hash=" + md5(filename.encode()).hexdigest()
        filename += ".parquet"
        return filename

def search_to_tmp_parquet(
        sentence: Union[List[str], str],
        df: pd.DataFrame,
        rootdir: Optional[str] = "eupol",
        **kwargs):
    rootdir = rootdir if rootdir else "parquet"
    tmp = tempfile.gettempdir()
    directory = os.path.join(tmp, rootdir, "dataframes")
    os.makedirs(directory, exist_ok=True)
    filename = _search_tmp_hash(sentence, **kwargs)
    filename = os.path.join(directory, filename)
    df.to_parquet(filename)

def search_from_tmp_parquet(
        sentence: Union[List[str], str],
        rootdir: Optional[str] = "eupol",
        **kwargs
        ):
    filename = _search_tmp_hash(sentence, **kwargs)
    rootdir = rootdir if rootdir else "parquet"
    tmp = tempfile.gettempdir()
    directory = os.path.join(tmp, rootdir, "dataframes")
    filename = os.path.join(directory, filename)
    if not os.path.exists(filename):
        return None
    else:
        return pd.read_parquet(filename)


class TopicFilter:
    def __init__(self,
        model: Optional[Model],
        agency_id: Optional[str] = None
        ):
        if agency_id and not model:
            self.model = Model(agency_id)
            self.agency_id = agency_id
        elif model and not agency_id:
            self.model = model
            self.agency_id = self.model.agency_id
        elif model and agency_id:
            self.model = model
            self.agency_id = self.model.agency_id
        else:
            raise ValueError("Either agency_id or model must be provided")
        
        self.state = self.model.init()
        self.searches = []
        self._old_state = None
        self._old_search = None
        self._old_search_cols = None

    def search(self,
        sentence: str,
        colname_substring: Optional[str] = "name",
        raw : Optional[bool] = False,
        literal_match_bonus: Optional[float] = 0.2,
        threshold_keep: Optional[float] = 0.4,
        ):
        _sentences = sentence if self.searches is None else [*self.searches, sentence]
        found = search_from_tmp_parquet(
            _sentences,
            colname_substring=colname_substring,
            raw=raw,
            literal_match_bonus=literal_match_bonus,
            threshold_keep=threshold_keep
            )
        if found is not None:
            self.curr_search = found
            self.curr_search_cols = [
            col for col in self.curr_search.columns
            if (
                colname_substring in col
                and not "search_" in col
                and not "reason_" in col
                )
            ]
            return self.curr_search
        
        self.curr_search_cols = [
            col for col in self.state.columns
            if (
                colname_substring in col
                and not "search_" in col
                and not "reason_" in col
                )
            ]
        dfsearch = self.state[self.curr_search_cols]
        
        def callback(sentence, df_value):
            if not df_value:
                return 0
            pval = parse(df_value)
            ratio = SequenceMatcher(
                None,
                sentence.lower(),
                pval
                ).ratio()
            if sentence.lower() in pval:
                ratio += literal_match_bonus
            return ratio

        self.curr_search = pd.concat(
            [
                dfsearch,
                dfsearch.apply(
                    lambda col: col.apply(
                        lambda x: callback(sentence, x)
                    ),
                ).rename(
                    columns={
                        col: f"search.{col}"
                        for col in dfsearch.columns
                        })
            ],
            axis=1
        )
        if raw:
            keep = self.curr_search.assign(reason_column="raw", reason_value="raw")
        else:
            self.curr_search.columns = [col.replace(".", "_") for col in self.curr_search.columns]
            search_cols = [col for col in self.curr_search.columns if "search_" in col]
            query_keep = " or ".join([f"{col} >= {threshold_keep}" for col in search_cols ])
            keep = self.curr_search.query(query_keep)
            reason = keep.apply(lambda row: [
                [ col.replace("search_", ""), val ] for col, val in row.items()
                if val == max([v for v in row.values if not isinstance(v, str) and not v is None])
                ].pop(),
                axis=1,
                result_type="expand"
                ).rename(columns={0: "reason_column", 1: "reason_value"})
            keep = keep.assign(reason_column=reason.reason_column, reason_value=reason.reason_value)
        
        # save for further use
        search_to_tmp_parquet(
            _sentences,
            keep,
            colname_substring=colname_substring,
            raw=raw,
            literal_match_bonus=literal_match_bonus,
            threshold_keep=threshold_keep
            )
        return keep

    
    def filter(self, sentence: str, **kwargs):
        # check if there is more to filter
        if len(self.state) == 1:
            rlog(f"❌ No more filtering possible.", style="bold red")
            return self
        if not len(self.state):
            rlog(f"❌ No content found, attempting to bracktrack...", style="bold red")
            return self.backtrack()
        # keep mem of (n-1)th state
        self._old_state = self.state
        if hasattr(self, "curr_search"):
            self._old_search = self.curr_search
            self._old_search_cols = self.curr_search_cols
        # update state
        kwargs["raw"] = False
        self.state = self.search(sentence, **kwargs)
        self.searches += [sentence]
        return deepcopy(self) # else it will be updated in place

    def state_tree(self):
        rprint(self.state[self.curr_search_cols])
        return self.model.ftoc.toc_tree(self.state[self.curr_search_cols])

    def __len__(self):
        return len(self.state)

    def __repr__(self):
        return repr(self.state)
    def __str__(self):
        return str(self.state)
    def __rich__(self):
        return self.state_tree()

    def traverse(self, sentences: List[str]):
        self.__init__(self.model)
        for st in sentences:
            self = self.filter(st)
        return self
    
    def backtrack(self, n=1):
        if n == 1:
            self.state = self._old_state
            self.curr_search = self._old_search
            self.curr_search_cols = self._old_search_cols
            self.searches = self.searches[:-1]
        else:
            searches = self.searches[:-n]
            self.traverse(searches)
        return self

    # def join_pars(self):
    #     pass
        
    # def __repr__(self):
    #     return self.state.__repr__()
    # def __str__(self):
    #     return self.state.__str__()

    # def __getitem__(self, key):
    #     return self.state[key]

    # def __contains__(self, kw):
    #     return len(
    #         self.state[
    #             self.state['title'].str.lower().str.contains(kw)
    #             ])


    # def load(self, max_tables: int = 10):
    #     if nbtables := len(self.state) > max_tables:
    #         rlog(f"❌ Too many tables to load ({nbtables}). Specify a smaller number.", style="bold red")
    #         rlog(f"ℹ Use the `relevant` method to see the most relevant tables.", style="bold blue")
    #         rlog(f"> Loading 10 tables from current selection...", style="blue")
    #         select = self.state.head(10)
    
    # def rmtoc(self):
    #     rmcache('toc')
    #     rlog("✅ Removed toc cache.", style="green")

    # def set_attrs(self):
    #     attrs = ["shape", "columns", "index", "dtypes", "memory_usage"]
    #     for attr in attrs:
    #         setattr(self, attr, getattr(self.state, attr))
    #     setattr(self, "pars", eurostat.get_pars)
    
    # def _set_kw_attrs(self):
    #     for kw in self.kws:
    #         setattr(self, kw, self.state[self.state['title'].str.lower().str.contains(kw)])

if __name__ == "__main__":
    estat = Model("ESTAT")
    toc = estat.init()
    tf = TopicFilter(estat)
    tfs1 = tf.filter("GDP").filter("sustainable")
    rprint(tfs1)
    tfs2 = tf.filter("GDP").filter("policy")
    rprint(tfs2)
    tfs3 = tf.filter("GDP").filter("policy").filter("sustainable")
    rprint(tfs3)
    tfs3.backtrack(2)
    rprint(tfs3)
    # for scol in tf.curr_search_cols:
    #     rprint(scol, search[[scol, "search."+scol]].sort_values(by="search."+scol, ascending=False).head(10))
    #     rprint(20*"-")
    # search.to_csv(data_dir("eupol").joinpath("search.csv"), index=False)