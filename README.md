# A project to help people make EU related maps

This project is a collection of tools to help people make maps of the EU. It is a work in progress and is not yet ready for use.

For now you have utils to [download NUTS data][1] and to convert it to GeoJSON or equivalent formats.

The basic utils are displayed in the [example notebook][2]. If you do not have a copy of the NUTS data when calling the utils, they will download it for you. If you do not specify a directory, they will store it in a temporary directory (which will disappear upon reboot).

The NUTS data can be easily visualized using the [GeoPandas][3] and [Folium][4] dependencies.

This is a sample map of every NUTS region in the EU: [NUTS map][5]

Here's a sneak peek

![NUTS Map][6]

# Notebook Example

![Notebook Example][2]

[1]:https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts#nuts21
[2]:assets/images/nuts-notebook.html
[3]:https://geopandas.org/
[4]:https://python-visualization.github.io/folium/
[5]:assets/images/nuts.html
[6]:assets/images/nuts.svg
