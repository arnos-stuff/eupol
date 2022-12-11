import pandasdmx as sdmx

from rich import print as rprint
from rich.tree import Tree
from eupol.download.sdmx.base import Model, sdmxBase

class DataSet(sdmxBase):
    def __new__(cls, model):
        cls.model = model
        cls.toc = cls.model.init()
        cls.query = sdmx.Request(cls.model.agency_id)
        return cls
    @classmethod
    def set(cls, dataflow: str):
        cls.dataflow = toc[toc['dataflow.id'] == dataflow]
        cls.codes = cls.model.descendants.df(data=cls.dataflow['dataflow.id'].values[0])
        return cls
    @classmethod
    def tree(cls):
        if not hasattr(cls, 'codes'):
            return "No dataflow selected"
        tree = Tree(f"[bold blue]> 🔎 The dataflow {first['dataflow.id']} has {len(cls.codes.parent.unique())} parameters to filter by:[/]")
        for code in cls.codes.parent.unique():
            codetree = tree.add(f"[bold red]{code} =[/]")
            for subcode in cls.codes[cls.codes.parent == code].id.unique():
                codetree.add(f"[bold yellow]{subcode}[/] [dim yellow]({cls.codes[cls.codes.id == subcode].name.values[0]})[/]")
        cls.tree_repr = tree
        return tree
    @classmethod
    def query(cls, **kwargs):
        return cls.query.data(
            cls.dataflow['dataflow.id'].values[0].lower(),
            key=kwargs
            )

estat = Model("ESTAT")
toc = estat.init()

sflows = toc.sample(5, random_state=42)
# rprint(sflows)

first = sflows.iloc[0]
# get the codelist for the first dataflow
rprint(first)

dataset = DataSet(estat).set(first['dataflow.id'])

rprint(dataset.tree())

# # Get the first dataflow
response = dataset.query.data(
    first["dataflow.id"].lower(),
    key={
        # "FREQ": "A",
        # "GEO": "EU27_2020",
        })

data = response.to_pandas()

print(data)

