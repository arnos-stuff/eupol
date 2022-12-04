import os
import sys
import requests
import json
import pandas as pd
import folium as fl

data = pd.read_csv("https://www.chesdata.eu/s/CHES2019_experts.csv")

dfm = pd.read_json("https://gisco-services.ec.europa.eu/distribution/v2/nuts/nuts-2021-files.json")
# Create a map
# m = fl.Map(location=[50.8503, 4.3517], zoom_start=6)
dfm.to_csv("metadata.csv")

