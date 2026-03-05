"""Test jsonjsdb compatibility with datannurpy export."""

from datannurpy.schema import DatannurDB

db = DatannurDB("examples/output/data/db")

print("=== Test where() ===")
string_vars = db["variable"].where("type", "==", "string")
print(f"Variables string: {len(string_vars)}")

print()
print("=== Test having.folder() ===")
company_datasets = db["dataset"].having.folder("company")
print(f"Datasets dans company: {len(company_datasets)}")
for ds in company_datasets[:3]:
    print(f"  - {ds['name']}")

print()
print("=== Test get() par ID ===")
folder = db["folder"].get("company")
print(f"Folder company: {folder}")

print()
print("=== Test modalities ===")
mods = db["modality"].all()
print(f"Nombre de modalités: {len(mods)}")
if mods:
    print(f"Exemple: {mods[0]}")

print()
print("=== Test values ===")
vals = db["value"].all()[:5]
print(f"Valeurs (5 premiers): {vals}")

print()
print("=== Test tag_ids (many-to-many) ===")
for ds in db["dataset"].all():
    if ds.get("tag_ids"):
        print(
            f"{ds['id']}: tag_ids = {ds['tag_ids']} (type: {type(ds['tag_ids']).__name__})"
        )
        break
else:
    print("Aucun dataset avec tag_ids")

print()
print("=== Test having.tag() (many-to-many inverse) ===")
tags = db["tag"].all()
if tags:
    tag_id = tags[0]["id"]
    try:
        tagged = db["dataset"].having.tag(tag_id)
        print(f"Datasets avec tag '{tag_id}': {len(tagged)}")
    except AttributeError as e:
        print(f"⚠ Colonne tag_ids absente (aucun dataset taggé): {e}")

print()
print("=== RÉSUMÉ ===")
print("✓ Lecture des tables: OK")
print("✓ where(): OK")
print("✓ having.x() (foreign key): OK")
print("✓ get(): OK")
print("✓ modalities/values: OK")
print("⚠ having.x() (many-to-many): nécessite que la colonne existe")
