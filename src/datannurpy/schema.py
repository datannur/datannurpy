"""Dataclass schemas and input models for datannur catalog."""

from __future__ import annotations

from dataclasses import dataclass, field

from jsonjsdb import Jsonjsdb, Table


@dataclass
class EntityMetadata:
    """Identity, parent linkage, and user metadata for a scanned entity."""

    id: str | None = None
    parent_id: str | None = None
    manager_organization_id: str | None = None
    owner_organization_id: str | None = None
    tag_ids: list[str] | None = None
    doc_ids: list[str] | None = None
    name: str | None = None
    description: str | None = None
    license: str | None = None
    type: str | None = None
    link: str | None = None
    localisation: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None


@dataclass
class Folder:
    """A folder containing datasets and sub-folders."""

    id: str
    parent_id: str | None = None
    manager_organization_id: str | None = None
    owner_organization_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    license: str | None = None
    type: str | None = None  # filesystem, sqlite, postgres, etc.
    link: str | None = None
    localisation: str | None = None
    data_path: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None
    last_update_date: str | None = None
    # Runtime field (not persisted)
    _seen: bool = False


def folder_from_metadata(
    metadata: EntityMetadata,
    *,
    default_id: str | None = None,
    default_name: str | None = None,
) -> Folder:
    """Build a Folder row from EntityMetadata with scan-derived defaults."""
    return Folder(
        id=metadata.id or default_id or "",
        parent_id=metadata.parent_id,
        manager_organization_id=metadata.manager_organization_id,
        owner_organization_id=metadata.owner_organization_id,
        tag_ids=list(metadata.tag_ids or []),
        doc_ids=list(metadata.doc_ids or []),
        name=metadata.name if metadata.name is not None else default_name,
        description=metadata.description,
        license=metadata.license,
        type=metadata.type,
        link=metadata.link,
        localisation=metadata.localisation,
        start_date=metadata.start_date,
        end_date=metadata.end_date,
        updating_each=metadata.updating_each,
        no_more_update=metadata.no_more_update,
    )


@dataclass
class Dataset:
    """A tabular data collection (table, file, etc.)."""

    id: str
    folder_id: str | None = None
    manager_organization_id: str | None = None
    owner_organization_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    license: str | None = None
    type: str | None = None
    data_path: str | None = None
    link: str | None = None
    localisation: str | None = None
    delivery_format: str | None = None
    nb_row: int | None = None
    sample_size: int | None = None
    preview_rows: int | None = None
    has_preview: int | None = None
    nb_resources: int | None = None  # Number of resources in series (>1 = time series)
    data_size: int | None = None  # Data size in bytes
    start_date: str | None = None
    end_date: str | None = None
    last_update_date: str | None = None
    updating_each: str | None = None
    no_more_update: str | None = None
    last_update_timestamp: int | None = None
    schema_signature: str | None = None
    # Runtime fields (not persisted)
    _seen: bool = False
    # Absolute path used to match this dataset to a physical file/directory.
    # Kept separate from exported data_path, which may be relative/public.
    _match_path: str | None = None


@dataclass
class Variable:
    """A column in a tabular dataset."""

    id: str
    name: str
    dataset_id: str
    enumeration_ids: list[str] = field(default_factory=list)
    tag_ids: list[str] = field(default_factory=list)
    source_variable_ids: list[str] = field(default_factory=list)
    fk_variable_id: str | None = None
    original_name: str | None = None
    description: str | None = None
    type: str | None = None
    key: int | None = None
    business_key: int | None = None
    nb_distinct: int | None = None
    nb_duplicate: int | None = None
    nb_missing: int | None = None
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None
    start_date: str | None = None
    end_date: str | None = None
    is_pattern: bool = False
    concept_id: str | None = None


@dataclass
class Enumeration:
    """A reusable set of categorical values."""

    id: str
    folder_id: str | None = None
    name: str | None = None
    description: str | None = None
    type: str | None = None
    # Runtime field (not persisted)
    _seen: bool = False


@dataclass
class Value:
    """A value within an enumeration."""

    id: str = ""  # Computed at runtime from enumeration_id + value
    enumeration_id: str = ""
    value: str | None = None
    description: str | None = None


@dataclass
class Frequency:
    """Frequency count for a variable value."""

    id: str = ""  # Computed at runtime from variable_id + value
    variable_id: str = ""
    value: str | None = None
    frequency: int = 0


@dataclass
class Organization:
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
    _seen: bool = False  # Runtime field for incremental scan


@dataclass
class Tag:
    """A keyword/tag for categorization."""

    id: str
    parent_id: str | None = None
    doc_ids: list[str] = field(default_factory=list)
    implied_tag_ids: list[str] = field(default_factory=list)
    propagate_to_parents: bool | None = None
    name: str | None = None
    description: str | None = None
    _seen: bool = False  # Runtime field for incremental scan


@dataclass
class Doc:
    """A document attached to entities."""

    id: str
    name: str | None = None
    description: str | None = None
    path: str | None = None
    type: str | None = None
    last_update: str | None = None
    _seen: bool = False  # Runtime field for incremental scan


@dataclass
class Concept:
    """A business glossary concept."""

    id: str
    parent_id: str | None = None
    tag_ids: list[str] = field(default_factory=list)
    doc_ids: list[str] = field(default_factory=list)
    name: str | None = None
    description: str | None = None
    _seen: bool = False  # Runtime field for incremental scan


@dataclass
class Config:
    """A key-value config entry for the web app."""

    id: str
    value: str = ""


@dataclass
class ConfigFilter:
    """A global database filter rule exposed in the web app."""

    id: str
    name: str
    entity: str
    field: str
    value: str | int | float | bool | None
    is_active_default: bool | None = None


class DatannurDB(Jsonjsdb):
    """Typed datannur database with all tables."""

    folder: Table[Folder]
    dataset: Table[Dataset]
    variable: Table[Variable]
    enumeration: Table[Enumeration]
    value: Table[Value]
    frequency: Table[Frequency]
    organization: Table[Organization]
    tag: Table[Tag]
    doc: Table[Doc]
    concept: Table[Concept]
    config: Table[Config]
    configFilter: Table[ConfigFilter]

    def __init__(self, path: str | None = None) -> None:
        super().__init__(path)
        # Set entity types manually (jsonjsdb doesn't extract from type hints yet)
        self.folder._entity_type = Folder
        self.dataset._entity_type = Dataset
        self.variable._entity_type = Variable
        self.enumeration._entity_type = Enumeration
        self.value._entity_type = Value
        self.frequency._entity_type = Frequency
        self.organization._entity_type = Organization
        self.tag._entity_type = Tag
        self.doc._entity_type = Doc
        self.concept._entity_type = Concept
        self.config._entity_type = Config
        self.configFilter._entity_type = ConfigFilter
        # Set runtime fields (not persisted)
        self.folder.runtime_fields = {"_seen"}
        self.dataset.runtime_fields = {"_seen", "_match_path", "preview_rows"}
        self.enumeration.runtime_fields = {"_seen"}
        self.organization.runtime_fields = {"_seen"}
        self.tag.runtime_fields = {"_seen"}
        self.doc.runtime_fields = {"_seen"}
        self.concept.runtime_fields = {"_seen"}
        self.value.runtime_fields = {"id"}
        self.frequency.runtime_fields = {"id"}
