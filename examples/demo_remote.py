import os
from pathlib import Path

from dotenv import load_dotenv

from datannurpy import Catalog, Folder

load_dotenv()

HERE = Path(__file__).parent
DATA = HERE.parent / "data"

# SFTP configuration from .env
REMOTE = os.environ["SFTP_URL"]
AUTH = {"key_filename": os.environ["SFTP_KEY"]}

catalog = Catalog(
    app_path=HERE / "output_remote",
    refresh=True,
    depth="value",
    metadata_path=DATA / "metadata",
)
catalog.add_folder(REMOTE, storage_options=AUTH)
catalog.add_database(f"{REMOTE}/company.db", storage_options=AUTH)
catalog.add_database(
    f"{REMOTE}/photovoltaik.gpkg",
    folder=Folder(
        id="photovoltaik",
        name="Grandes installations photovoltaïques",
        description="Installations photovoltaïques de haute altitude en Suisse. "
        "Source: Office fédéral de l'énergie (OFEN) - opendata.swiss",
    ),
    storage_options=AUTH,
)
catalog.export_app(open_browser=True)
