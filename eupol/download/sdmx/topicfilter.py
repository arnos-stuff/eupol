import os
import pickle
import tempfile
from typing import List, Union, Dict, Any, Optional
from rich.tree import Tree
from rich import print as rprint
from hashlib import sha512 as sha


from eupol.download.utils import rlog, rc
from eupol.download.sdmx.base import Model

estat = Model("ESTAT")
toc = estat.init()                
rprint(estat.ftoc.toc_tree())
# class TopicFilter:
#     def __init__(self, agency_id: str,  topic: Optional[str] = None):
#         self.agency_id = agency_id
#         self.model = Model(agency_id)
#         if topic is None:
#             self.state = self.model.init()
#             self.topics = []
#             self._old_state = None
#             self.kws = text.tokens(self.state, column='title')
#         else:
#             filtered = self.filter(self.state, topic)
#             self.topics = filtered.topics
#             self.state = filtered.state
#             self._old_state = self.model.init()
    
#     def filter(self, topic: str):
#         # check if there is more to filter
#         if len(self.state) == 1:
#             rlog(f"❌ No more filtering possible.", style="bold red")
#             return self
#         if not len(self.state):
#             rlog(f"❌ No content found, attempting to bracktrack...", style="bold red")
#             return self.backtrack()
#         # keep mem of (n-1)th state
#         self._old_state = self.state
#         self.old_kws = self.kws
#         # update state
#         self.topics += [topic]
#         self.state = eurostat.subset_toc_df(self.state, topic)
#         self.kws = text.tokens(self.state, column='title')
#         self.set_attrs()
#         return self

#     def __len__(self):
#         return len(self.state)
    
#     def relevant(self, n=5):
#         return self.kws.sort_values(by="count", ascending=False).head(n)

#     def traverse(self, topics: List[str]):
#         self.__init__(topic=topics.pop(0))
#         for topic in topics:
#             self.filter(topic)
    
#     def backtrack(self, n=1):
#         if n == 1:
#             self.state = self._old_state
#             self.kws = self.old_kws
#             self.topics = self.topics[:-1]
#             self.set_attrs()
#         else:
#             topics = self.topics[:-n]
#             self.traverse(topics)
#             self.set_attrs()
#         return self

#     def join_pars(self):
#         pass
        
#     def __repr__(self):
#         return self.state.__repr__()
#     def __str__(self):
#         return self.state.__str__()

#     def __getitem__(self, key):
#         return self.state[key]

#     def __contains__(self, kw):
#         return len(
#             self.state[
#                 self.state['title'].str.lower().str.contains(kw)
#                 ])


#     def load(self, max_tables: int = 10):
#         if nbtables := len(self.state) > max_tables:
#             rlog(f"❌ Too many tables to load ({nbtables}). Specify a smaller number.", style="bold red")
#             rlog(f"ℹ Use the `relevant` method to see the most relevant tables.", style="bold blue")
#             rlog(f"> Loading 10 tables from current selection...", style="blue")
#             select = self.state.head(10)
    
#     def rmtoc(self):
#         rmcache('toc')
#         rlog("✅ Removed toc cache.", style="green")

#     def set_attrs(self):
#         attrs = ["shape", "columns", "index", "dtypes", "memory_usage"]
#         for attr in attrs:
#             setattr(self, attr, getattr(self.state, attr))
#         setattr(self, "pars", eurostat.get_pars)
    
#     def _set_kw_attrs(self):
#         for kw in self.kws:
#             setattr(self, kw, self.state[self.state['title'].str.lower().str.contains(kw)])
