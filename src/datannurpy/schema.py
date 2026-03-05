"""Dataclass schemas for datannur catalog (jsonjsdb integration)."""

from __future__ import annotations

from dataclasses import dataclass, field

from jsonjsdb import Jsonjsdb, Table


@dataclass
class Folder:
    """A folder containing datasets and sub-folders."""

    id: str
    parent_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    type: str | None = None  # filesystem, sqlite, postgres, etc.
    data_path: str | None = None
    last_update_date: str | None = None


@dataclass
class Dataset:
    """A tabular data collection (table, file, etc.)."""

    id: str
    folder_id: str | None = None
    manager_id: str | None = None
    owner_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    type: str | None = None
    data_path: str | None = None
    link: str | None = None
    localisation: str | None = None
    delivery_format: str | None = None
    nb_row: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    last_update_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None
    last_update_timestamp: int | None = None
    schema_signature: str | None = None


@dataclass
class Variable:
    """A column in a tabular dataset."""

    id: str
    name: str
    dataset_id: str
    modality_ids: list[str] = field(default_factory=list)
    tag_ids: list[str] = field(default_factory=list)
    source_var_ids: list[str] = field(default_factory=list)
    original_name: str | None = None
    description: str | None = None
    type: str | None = None
    key: int | None = None
    nb_distinct: int | None = None
    nb_duplicate: int | None = None
    nb_missing: int | None = None
    start_date: str | None = None
    end_date: str | None = None


@dataclass
class Modality:
    """A reusable set of categorical values."""

    id: str
    folder_id: str | None = None
    name: str | None = None
    description: str | None = None
    type: str | None = None


@dataclass
class Value:
    """A value within a modality (no unique id)."""

    modality_id: str
    value: str | None = None
    description: str | None = None


@dataclass
class Freq:
    """Frequency count for a variable value (no unique id)."""

    variable_id: str
    value: str
    freq: int


@dataclass
class Institution:
    """An organization that manages data."""

    id: str
    parent_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    email: str | None = None
    phone: str | None = None
    start_date: str | None = None
    end_date: str | None = None


@dataclass
class Tag:
    """A keyword/tag for categorization."""

    id: str
    parent_id: str | None = None
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None


@dataclass
class Doc:
    """A document attached to entities."""

    id: str
    name: str | None = None
    description: str | None = None
    path: str | None = None
    type: str | None = None
    last_update: str | None = None


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
