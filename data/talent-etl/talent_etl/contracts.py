"""
Schema contracts for every source entity: source schema, Silver schema and keys.

The PostgreSQL contracts mirror the provided schema.sql exactly (a unit test parses
schema.sql from the sample zip and compares). The learning_activity contract is
derived from the observed JSONL structure. docs/schema_contracts.md explains each
contract; docs/source_to_target.md maps it to Bronze and Silver paths.

Enforcement levels:
    allowed_values / min_value / max_value / pattern   enforced -> quarantine (DOM-001/002/003)
    expected_values / expected_pattern / expected_*     observed only -> warn (DOM-004/005/006)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import pyarrow as pa

from talent_etl.definitions import LEARNING_EVENT_TYPES


class Kind(StrEnum):
    """Parser used for a column (see talent_etl.parsing)."""

    INT = "int"
    FLOAT = "float"
    DECIMAL = "decimal"
    STRING = "string"
    DATE = "date"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True)
class ColumnContract:
    """One column: its source definition, Silver type and validation constraints."""

    name: str
    source_type: str
    silver_type: pa.DataType
    kind: Kind
    required: bool
    description: str = ""
    allowed_values: tuple[str, ...] = ()
    expected_values: tuple[str, ...] = ()
    min_value: float | None = None
    max_value: float | None = None
    expected_min: float | None = None
    expected_max: float | None = None
    pattern: str | None = None
    expected_pattern: str | None = None
    aliases: tuple[str, ...] = ()
    not_after_ingest_date: bool = False


@dataclass(frozen=True)
class ForeignKey:
    """A reference from ``column`` to ``references.ref_column``."""

    column: str
    references: str
    ref_column: str


@dataclass(frozen=True)
class DerivedColumn:
    """A column Silver adds to the source columns; ``rule`` documents how it is computed."""

    name: str
    silver_type: pa.DataType
    rule: str


@dataclass(frozen=True)
class EntityContract:
    """The full contract for one source entity."""

    name: str
    source_system: str
    source_object: str
    description: str
    columns: tuple[ColumnContract, ...]
    primary_key: tuple[str, ...]
    silver_name: str
    natural_key: tuple[str, ...] = ()
    dedup_order: tuple[str, ...] = ()
    unique_warn: tuple[str, ...] = ()
    foreign_keys: tuple[ForeignKey, ...] = ()
    derived_columns: tuple[DerivedColumn, ...] = ()
    partition_by: tuple[str, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        """Source column names in contract order."""
        return tuple(column.name for column in self.columns)

    @property
    def required_columns(self) -> tuple[str, ...]:
        """Names of columns that must be present and non-null."""
        return tuple(column.name for column in self.columns if column.required)

    def column(self, name: str) -> ColumnContract:
        """Return the contract for one column."""
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(f"{self.name} has no column {name!r}")

    def silver_schema(self) -> pa.Schema:
        """
        Return the logical Silver schema: source columns, derived columns, then lineage/DQ columns.

        Partition columns are part of the logical schema but are stored only in the
        hive path (``event_year=2024/``), never inside the files; see ``silver_file_schema``.
        """
        fields = [pa.field(c.name, c.silver_type, nullable=not c.required) for c in self.columns]
        fields += [pa.field(d.name, d.silver_type, nullable=True) for d in self.derived_columns]
        lineage = SILVER_LINEAGE_FIELDS_JSONL if self.source_system == "s3_jsonl" else SILVER_LINEAGE_FIELDS
        return pa.schema(fields + list(lineage))

    def silver_file_schema(self) -> pa.Schema:
        """Return the schema of each Silver Parquet file: the logical schema minus partition columns."""
        schema = self.silver_schema()
        for name in self.partition_by:
            schema = schema.remove(schema.get_field_index(name))
        return schema


# Lineage / data quality columns appended to every Silver dataset.
SILVER_LINEAGE_FIELDS = (
    pa.field("_run_id", pa.string(), nullable=False),
    pa.field("_bronze_ingest_date", pa.date32(), nullable=False),
    pa.field("_source_object", pa.string(), nullable=False),
    pa.field("_processed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_dq_warnings", pa.list_(pa.string()), nullable=False),
)
SILVER_LINEAGE_FIELDS_JSONL = (
    *SILVER_LINEAGE_FIELDS[:3],
    pa.field("_source_line", pa.int32(), nullable=False),
    *SILVER_LINEAGE_FIELDS[3:],
)

# Every rejected record, from any entity, lands in its entity's quarantine dataset with this schema.
QUARANTINE_SCHEMA = pa.schema([
    pa.field("entity", pa.string(), nullable=False),
    pa.field("source_object", pa.string(), nullable=False),   # PG table or S3 key
    pa.field("source_line", pa.int32(), nullable=True),       # JSONL line number (1-based)
    pa.field("record_key", pa.string(), nullable=True),       # primary key value(s), best effort
    pa.field("raw_record", pa.string(), nullable=False),      # original line / row as JSON
    pa.field("rule_ids", pa.list_(pa.string()), nullable=False),
    pa.field("reason_codes", pa.list_(pa.string()), nullable=False),
    pa.field("reason_detail", pa.string(), nullable=False),
    pa.field("_run_id", pa.string(), nullable=False),
    pa.field("_bronze_ingest_date", pa.date32(), nullable=False),
    pa.field("_quarantined_at", pa.timestamp("us", tz="UTC"), nullable=False),
])

_ID = {"min_value": 1}
_PERIOD_PATTERN = r"^\d{4}-H[12]$"
_PRE_HIRE = DerivedColumn("is_pre_hire", pa.bool_(), "see business rules BUS-001..004")


def _pk(name: str, description: str) -> ColumnContract:
    return ColumnContract(name, "INTEGER PRIMARY KEY", pa.int32(), Kind.INT, True, description, **_ID)


def _fk(name: str, references: str, description: str) -> ColumnContract:
    return ColumnContract(name, f"INTEGER NOT NULL REFERENCES {references}", pa.int32(), Kind.INT, True,
                          description, **_ID)


def _text(name: str, description: str, required: bool = True, **constraints) -> ColumnContract:
    source_type = "TEXT NOT NULL" if required else "TEXT"
    if "allowed_values" in constraints:
        source_type += " CHECK IN (...)"
    return ColumnContract(name, source_type, pa.string(), Kind.STRING, required, description, **constraints)


def _date(name: str, description: str) -> ColumnContract:
    return ColumnContract(name, "DATE NOT NULL", pa.date32(), Kind.DATE, True, description,
                          not_after_ingest_date=True)


def _level(name: str, description: str) -> ColumnContract:
    return ColumnContract(name, "INTEGER NOT NULL CHECK 1-5", pa.int32(), Kind.INT, True, description,
                          min_value=1, max_value=5)


TEAMS = EntityContract(
    name="teams", source_system="postgres", source_object="hr_source.teams", silver_name="teams",
    description="Organisational teams; each belongs to one department.",
    columns=(
        _pk("team_id", "Team identifier"),
        _text("name", "Team display name"),
        _text("department", "Department name", expected_values=(
            "Customer Support", "Data & Analytics", "Design", "Engineering", "Finance",
            "Human Resources", "Marketing", "Operations", "Product", "Sales")),
    ),
    primary_key=("team_id",),
)

COMPETENCIES = EntityContract(
    name="competencies", source_system="postgres", source_object="hr_source.competencies",
    silver_name="competencies", description="Competency catalogue.",
    columns=(
        _pk("competency_id", "Competency identifier"),
        _text("name", "Competency name"),
        _text("category", "Competency category", allowed_values=("Technical", "Leadership", "Domain")),
    ),
    primary_key=("competency_id",),
)

EMPLOYEES = EntityContract(
    name="employees", source_system="postgres", source_object="hr_source.employees", silver_name="employees",
    description="Employee master data (one row per employee).",
    columns=(
        _pk("employee_id", "Employee identifier"),
        _text("first_name", "Given name"),
        _text("last_name", "Family name"),
        _text("email", "Work email", expected_pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        _fk("team_id", "teams(team_id)", "Current team"),
        _text("job_level", "Job level", expected_values=(
            "IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director")),
        _date("hire_date", "Hire date"),
    ),
    primary_key=("employee_id",),
    unique_warn=("email",),
    foreign_keys=(ForeignKey("team_id", "teams", "team_id"),),
)

PROJECTS = EntityContract(
    name="projects", source_system="postgres", source_object="hr_source.projects", silver_name="projects",
    description="Projects and their delivery outcome; linked to teams only (not employees). "
                "Names are not unique (e.g. 'MediumSeaGreen Rollout' x2): join on project_id.",
    columns=(
        _pk("project_id", "Project identifier"),
        _text("name", "Project name (not unique)"),
        _fk("team_id", "teams(team_id)", "Owning team"),
        _text("outcome_status", "Delivery status",
              allowed_values=("on-time", "delayed", "at-risk", "cancelled")),
        ColumnContract("outcome_score", "NUMERIC(5,2) NOT NULL", pa.decimal128(5, 2), Kind.DECIMAL, True,
                       "Outcome score", expected_min=0, expected_max=100),
    ),
    primary_key=("project_id",),
    foreign_keys=(ForeignKey("team_id", "teams", "team_id"),),
)

PERFORMANCE_REVIEWS = EntityContract(
    name="performance_reviews", source_system="postgres", source_object="hr_source.performance_reviews",
    silver_name="performance_reviews", description="Half-yearly performance ratings.",
    columns=(
        _pk("review_id", "Review identifier"),
        _fk("employee_id", "employees(employee_id)", "Reviewed employee"),
        _text("review_period", "Half-year period YYYY-H1 or YYYY-H2", pattern=_PERIOD_PATTERN),
        _level("rating", "Performance rating 1 (lowest) to 5 (highest)"),
        _text("manager_comment", "Free-text manager comment", required=False),
        _date("submitted_date", "Date the review was submitted"),
    ),
    primary_key=("review_id",),
    natural_key=("employee_id", "review_period"),
    dedup_order=("submitted_date", "review_id"),
    foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    derived_columns=(
        DerivedColumn("period_start", pa.date32(), "first day of review_period"),
        DerivedColumn("period_end", pa.date32(), "last day of review_period"),
        _PRE_HIRE,
    ),
)

COMPETENCY_ASSESSMENTS = EntityContract(
    name="competency_assessments", source_system="postgres", source_object="hr_source.competency_assessments",
    silver_name="competency_assessments", description="Assessed vs required proficiency per competency.",
    columns=(
        _pk("assessment_id", "Assessment identifier"),
        _fk("employee_id", "employees(employee_id)", "Assessed employee"),
        _fk("competency_id", "competencies(competency_id)", "Assessed competency"),
        _level("proficiency_level", "Assessed proficiency 1-5"),
        _level("required_level", "Proficiency required for the role 1-5"),
        _date("assessed_date", "Assessment date"),
    ),
    primary_key=("assessment_id",),
    natural_key=("employee_id", "competency_id", "assessed_date"),
    dedup_order=("assessment_id",),
    foreign_keys=(
        ForeignKey("employee_id", "employees", "employee_id"),
        ForeignKey("competency_id", "competencies", "competency_id"),
    ),
    derived_columns=(
        DerivedColumn("gap", pa.int32(), "required_level - proficiency_level"),
        DerivedColumn("is_critical_gap", pa.bool_(), "gap / required_level >= CRITICAL_GAP_RATIO (1/2)"),
        _PRE_HIRE,
    ),
)

TALENT_REVIEW = EntityContract(
    name="talent_review", source_system="postgres", source_object="hr_source.talent_review",
    silver_name="talent_review", description="Latest talent review per employee (one cycle in the data).",
    columns=(
        ColumnContract("employee_id", "INTEGER PRIMARY KEY REFERENCES employees(employee_id)", pa.int32(),
                       Kind.INT, True, "Reviewed employee", min_value=1),
        _text("review_cycle", "Half-year cycle YYYY-H1 or YYYY-H2", pattern=_PERIOD_PATTERN),
        _text("potential_rating", "Potential", allowed_values=("Low", "Medium", "High")),
        _text("promotion_readiness", "Promotion readiness", allowed_values=("Ready Now", "1-2 Years", "Not Ready")),
        _text("attrition_risk", "HR-assessed attrition risk", allowed_values=("Low", "Medium", "High")),
    ),
    primary_key=("employee_id",),
    foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    derived_columns=(
        DerivedColumn("cycle_start", pa.date32(), "first day of review_cycle"),
        DerivedColumn("cycle_end", pa.date32(), "last day of review_cycle"),
        _PRE_HIRE,
    ),
)

LEARNING_ACTIVITY = EntityContract(
    name="learning_activity", source_system="s3_jsonl",
    source_object="learning_activity/year=YYYY/month=MM/day=DD/*.jsonl",
    silver_name="learning_activity", description="Learning platform events, one JSON object per line.",
    columns=(
        ColumnContract("event_id", "JSON string", pa.string(), Kind.STRING, True, "Event identifier",
                       expected_pattern=r"^EV\d{10}$"),
        ColumnContract("employee_id", "JSON string of digits (e.g. \"989\")", pa.int32(), Kind.INT, True,
                       "Employee; cast to int to join employees", min_value=1),
        ColumnContract("event_date", "JSON string, ISO 8601 date-time without offset", pa.timestamp("us"),
                       Kind.TIMESTAMP, True, "When the event happened", not_after_ingest_date=True),
        ColumnContract("event_type", "JSON string", pa.string(), Kind.STRING, True, "Funnel step",
                       allowed_values=LEARNING_EVENT_TYPES),
        ColumnContract("course_name", "JSON string", pa.string(), Kind.STRING, True, "Course title"),
        ColumnContract("text", "JSON string (drifted key: notes)", pa.string(), Kind.STRING, False,
                       "Human-readable description", aliases=("notes",)),
        ColumnContract("score", "JSON integer or null", pa.float64(), Kind.FLOAT, False,
                       "Quiz score; only on quiz events", min_value=0, max_value=100),
    ),
    primary_key=("event_id",),
    foreign_keys=(ForeignKey("employee_id", "employees", "employee_id"),),
    derived_columns=(
        # int32 because that is what hive-partition readers infer from event_year=2024/.
        DerivedColumn("event_year", pa.int32(), "year of event_date (partition column, stored in the path only)"),
        DerivedColumn("event_month", pa.int32(), "month of event_date (partition column, stored in the path only)"),
        _PRE_HIRE,
    ),
    partition_by=("event_year", "event_month"),
)

# Validation order: parents before children, so foreign keys resolve against accepted parents.
CONTRACTS: dict[str, EntityContract] = {
    contract.name: contract for contract in (
        TEAMS, COMPETENCIES, EMPLOYEES, PROJECTS,
        PERFORMANCE_REVIEWS, COMPETENCY_ASSESSMENTS, TALENT_REVIEW, LEARNING_ACTIVITY,
    )
}
