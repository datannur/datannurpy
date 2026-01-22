from pathlib import Path

from datannurpy import Catalog

HERE = Path(__file__).parent
DATA = HERE.parent / "data"

catalog = Catalog()
catalog.add_folder(DATA)
catalog.add_database(f"sqlite:///{DATA}/company.db")
catalog.export_app(HERE / "output", open_browser=True)

print(f"✅ {catalog}")
