"""Tests for evolution tracking stability across successive runs."""

import json
import sqlite3
from pathlib import Path

from datannurpy import Catalog, Folder


class TestEvolutionStability:
    """Test that evolution table is stable across successive runs."""

    def test_successive_runs_file_no_spurious_evolution(self, tmp_path: Path):
        """Running twice on unchanged files should not create new evolution entries."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        data_dir = tmp_path / "source"
        data_dir.mkdir()

        # Create a simple CSV file
        (data_dir / "data.csv").write_text("id,name\n1,Alice\n2,Bob\n")

        # Run 1
        catalog1 = Catalog(app_path=app_dir)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # No evolution on first run
        assert not (db_dir / "evolution.json").exists()

        # Run 2 - identical (no file changes)
        catalog2 = Catalog(app_path=app_dir)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.export_db()

        # Still no evolution (no changes)
        assert not (db_dir / "evolution.json").exists()

        # Run 3 - just to be sure
        catalog3 = Catalog(app_path=app_dir)
        catalog3.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog3.export_db()

        assert not (db_dir / "evolution.json").exists()

    def test_successive_runs_database_no_spurious_evolution(self, tmp_path: Path):
        """Running twice on unchanged database should not create spurious evolution.

        This specifically tests that last_update_timestamp doesn't generate
        spurious evolution entries when the database hasn't actually changed.
        """
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        database_path = tmp_path / "test.db"

        # Create a simple SQLite database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE users (id INT, name TEXT)")
        cursor.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        conn.commit()
        conn.close()

        conn_str = f"sqlite:///{database_path}"

        # Run 1
        catalog1 = Catalog(app_path=app_dir)
        catalog1.add_database(conn_str, Folder(id="mydb", name="My Database"))
        catalog1.export_db()

        # No evolution on first run
        assert not (db_dir / "evolution.json").exists()

        # Run 2 - identical (no database changes)
        catalog2 = Catalog(app_path=app_dir)
        catalog2.add_database(conn_str, Folder(id="mydb", name="My Database"))
        catalog2.export_db()

        # Check if evolution was created
        if (db_dir / "evolution.json").exists():
            with open(db_dir / "evolution.json") as f:
                evolution = json.load(f)
            # If evolution exists, check it's not spurious timestamp updates
            timestamp_updates = [
                e for e in evolution if e.get("variable") == "last_update_timestamp"
            ]
            assert len(timestamp_updates) == 0, (
                f"Spurious timestamp updates: {timestamp_updates}"
            )

        # Run 3
        catalog3 = Catalog(app_path=app_dir)
        catalog3.add_database(conn_str, Folder(id="mydb", name="My Database"))
        catalog3.export_db()

        if (db_dir / "evolution.json").exists():
            with open(db_dir / "evolution.json") as f:
                evolution = json.load(f)
            timestamp_updates = [
                e for e in evolution if e.get("variable") == "last_update_timestamp"
            ]
            assert len(timestamp_updates) == 0, (
                f"Evolution should be stable: {timestamp_updates}"
            )

    def test_evolution_count_stability(self, tmp_path: Path):
        """Evolution count should remain stable across multiple identical runs."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        database_path = tmp_path / "test.db"

        # Create database
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE items (id INT, value TEXT)")
        cursor.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b')")
        conn.commit()
        conn.close()

        conn_str = f"sqlite:///{database_path}"
        evolution_counts: list[int] = []

        for i in range(5):
            catalog = Catalog(app_path=app_dir)
            catalog.add_database(conn_str, Folder(id="db", name="Database"))
            catalog.export_db()

            if (db_dir / "evolution.json").exists():
                with open(db_dir / "evolution.json") as f:
                    evolution = json.load(f)
                evolution_counts.append(len(evolution))
            else:
                evolution_counts.append(0)

        # After first run (0), evolution count should remain stable
        # i.e. not keep growing with each run
        if evolution_counts[0] == 0:
            # If no initial evolution, subsequent should also be 0
            assert all(c == 0 for c in evolution_counts), (
                f"Evolution keeps growing: {evolution_counts}"
            )
        else:
            # If there is evolution, it should stabilize after first run
            # Count should be the same after run 2+
            stable_counts = evolution_counts[1:]
            assert len(set(stable_counts)) == 1, (
                f"Evolution count not stable: {evolution_counts}"
            )

    def test_enumerations_folder_not_deleted_on_rerun(self, tmp_path: Path):
        """_enumerations folder should not be deleted on successive runs with refresh=True."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"
        data_dir = tmp_path / "source"
        data_dir.mkdir()

        # Create CSV with categorical data that generates enumerations
        (data_dir / "colors.csv").write_text("id,color\n1,red\n2,blue\n3,red\n")

        # Run 1 - creates enumerations and _enumerations folder
        catalog1 = Catalog(app_path=app_dir, refresh=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Verify _enumerations folder exists
        with open(db_dir / "folder.json") as f:
            folders1 = json.load(f)
        enumerations_folder = [f for f in folders1 if f["id"] == "_enumerations"]
        assert len(enumerations_folder) == 1, "_enumerations folder should exist"

        # No evolution on first run
        assert not (db_dir / "evolution.json").exists()

        # Run 2 - with refresh=True, _enumerations folder should NOT be deleted
        catalog2 = Catalog(app_path=app_dir, refresh=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.export_db()

        # Check evolution - should NOT contain delete of _enumerations
        if (db_dir / "evolution.json").exists():
            with open(db_dir / "evolution.json") as f:
                evolution = json.load(f)
            enumerations_deletes = [
                e
                for e in evolution
                if e.get("entity_id") == "_enumerations" and e.get("type") == "delete"
            ]
            assert len(enumerations_deletes) == 0, (
                "_enumerations folder should not be deleted on rerun"
            )

        # _enumerations folder should still exist
        with open(db_dir / "folder.json") as f:
            folders2 = json.load(f)
        enumerations_folder = [f for f in folders2 if f["id"] == "_enumerations"]
        assert len(enumerations_folder) == 1, "_enumerations folder should still exist"
