"""TypedDict schemas for datannur catalog (jsonjsdb integration)."""

from __future__ import annotations

from typing import TypedDict

from jsonjsdb import Jsonjsdb, Table


class Folder(TypedDict, total=False):
    """A folder containing datasets and sub-folders."""

    id: str  # Required
    parent_id: str | None
    tag_ids: list[str]
    doc_ids: list[str]
    name: str | None
    description: str | None
    type: str | None  # filesystem, sqlite, postgres, etc.
    data_path: str | None
    last_update_date: str | None


class Dataset(TypedDict, total=False):
    """A tabular data collection (table, file, etc.)."""

    id: str  # Required
    folder_id: str | None
    manager_id: str | None
    owner_id: str | None
    tag_ids: list[str]
    doc_ids: list[str]
    name: str | None
    description: str | None
    type: str | None
    data_path: str | None
    link: str | None
    localisation: str | None
    delivery_format: str | None
    nb_row: int | None
    start_date: str | None
    end_date: str | None
    last_update_date: str | None
    updating_each: str | None
    no_more_update: str | None
    last_update_timestamp: int | None
    schema_signature: str | None


class Variable(TypedDict, total=False):
    """A column in a tabular dataset."""

    id: str  # Required
    name: str  # Required
    dataset_id: str  # Required
    modality_ids: list[str]
    tag_ids: list[str]
    source_var_ids: list[str]
    original_name: str | None
    description: str | None
    type: str | None
    key: int | None
    nb_distinct: int | None
    nb_duplicate: int | None
    nb_missing: int | None
    start_date: str | None
    end_date: str | None


class Modality(TypedDict, total=False):
    """A reusable set of categorical values."""

    id: str  # Required
    folder_id: str | None
    name: str | None
    description: str | None
    type: str | None


class Value(TypedDict, total=False):
    """A value within a modality (no unique id)."""

    modality_id: str  # Required
    value: str | None
    description: str | None


class Freq(TypedDict, total=False):
    """Frequency count for a variable value (no unique id)."""

    variable_id: str  # Required
    value: str  # Required
    freq: int  # Required


class Institution(TypedDict, total=False):
    """An organization that manages data."""

    id: str  # Required
    parent_id: str | None
    tag_ids: list[str]
    doc_ids: list[str]
    name: str | None
    description: str | None
    email: str | None
    phone: str | None
    start_date: str | None
    end_date: str | None


class Tag(TypedDict, total=False):
    """A keyword/tag for categorization."""

    id: str  # Required
    parent_id: str | None
    doc_ids: list[str]
    name: str | None
    description: str | None


class Doc(TypedDict, total=False):
    """A document attached to entities."""

    id: str  # Required
    name: str | None
    description: str | None
    path: str | None
    type: str | None
    last_update: str | None


class DatannurDB(Jsonjsdb):
    """Typed datannur database with all tables."""

    folder: Table[Folder]
    dataset: Table[Dataset]
    variable: Table[Variable]
    modality: Table[Modality]
    value: Table[Value]
    freq: Table[Freq]
    institution: Table[Institution]
    tag: Table[Tag]
    doc: Table[Doc]

    def __init__(self, path: str | None = None) -> None:
        super().__init__(path)
        # Set runtime fields (not persisted)
        self.folder.runtime_fields = {"_seen"}
        self.dataset.runtime_fields = {"_seen"}
        self.modality.runtime_fields = {"_seen"}
        self.institution.runtime_fields = {"_seen"}
        self.tag.runtime_fields = {"_seen"}
        self.doc.runtime_fields = {"_seen"}
