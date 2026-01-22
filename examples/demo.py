from pathlib import Path

from datannurpy import Catalog, Folder

HERE = Path(__file__).parent
DATA = HERE.parent / "data"

catalog = Catalog()
catalog.add_folder(DATA)
catalog.add_database(f"sqlite:///{DATA}/company.db")
catalog.add_database(
    f"sqlite:///{DATA}/photovoltaik.gpkg",
    folder=Folder(
        id="photovoltaik",
        name="Grandes installations photovoltaïques",
        description="Installations photovoltaïques de haute altitude en Suisse. "
        "Source: Office fédéral de l'énergie (OFEN) - opendata.swiss",
    ),
)
catalog.export_app(HERE / "output", open_browser=True)

print(f"✅ {catalog}")
