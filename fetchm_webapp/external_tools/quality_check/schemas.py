from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_QUALITY_THRESHOLDS: dict[str, float | int | None] = {
    "min_completeness": 90.0,
    "max_contamination": 5.0,
    "max_n_percent": 5.0,
    "max_contigs": None,
    "min_n50": None,
    "min_ani_percent": 95.0,
    "max_mash_distance": None,
}

DEFAULT_QC_FILTER_MODE = "review_all"


@dataclass(frozen=True)
class QualityModule:
    key: str
    label: str
    category: str
    description: str
    default_enabled: bool = False
    requires_external_tool: bool = False
    tool_name: str = ""
    output_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


QUALITY_MODULES: tuple[QualityModule, ...] = (
    QualityModule(
        key="quick_fasta",
        label="Quick FASTA statistics",
        category="Built-in",
        description="Computes downloaded FASTA length, contig count, N50, GC%, and ambiguous-N percentage.",
        default_enabled=True,
        output_note="Runs inside FetchM Web without external databases.",
    ),
    QualityModule(
        key="checkm2",
        label="CheckM2 completeness/contamination",
        category="External",
        description="Runs or imports CheckM2 genome-quality estimates for completeness and contamination.",
        requires_external_tool=True,
        tool_name="checkm2",
        output_note="Requires CheckM2 and its database through a configured Nextflow profile.",
    ),
    QualityModule(
        key="quast",
        label="QUAST assembly structure",
        category="External",
        description="Adds detailed assembly metrics such as N50, total length, contig count, and GC content.",
        requires_external_tool=True,
        tool_name="quast.py",
        output_note="Requires QUAST through a configured Nextflow profile.",
    ),
    QualityModule(
        key="ani",
        label="ANI/species consistency",
        category="External",
        description="Screens species consistency and near outliers with FastANI or skani-style outputs.",
        requires_external_tool=True,
        tool_name="skani",
        output_note="Requires FastANI/skani through a configured Nextflow profile.",
    ),
    QualityModule(
        key="mash",
        label="Mash distance screen",
        category="External",
        description="Optional fast sketch-based duplicate/outlier pre-screen before heavier analysis.",
        requires_external_tool=True,
        tool_name="mash",
        output_note="Requires Mash through a configured Nextflow profile.",
    ),
    QualityModule(
        key="gtdbtk",
        label="GTDB-Tk taxonomy check",
        category="External",
        description="Optional taxonomy consistency check against GTDB-Tk reference data.",
        requires_external_tool=True,
        tool_name="gtdbtk",
        output_note="Requires GTDB-Tk and GTDB reference data; disabled by default because it is heavy.",
    ),
)


QUALITY_PROFILES: dict[str, dict[str, Any]] = {
    "quick": {
        "label": "Quick QC",
        "description": "Built-in FASTA and available CheckM metadata checks. Best for fast filtering.",
        "modules": ["quick_fasta"],
        "run_mode": "quick",
    },
    "standard": {
        "label": "Standard external QC",
        "description": "Quick QC plus CheckM2 and QUAST handoff/execution when external tools are configured.",
        "modules": ["quick_fasta", "checkm2", "quast"],
        "run_mode": "handoff",
    },
    "species_screen": {
        "label": "Species consistency QC",
        "description": "Adds ANI/species-consistency checks for downstream comparative genomics.",
        "modules": ["quick_fasta", "checkm2", "quast", "ani"],
        "run_mode": "handoff",
    },
    "comprehensive": {
        "label": "Comprehensive QC",
        "description": "PanResistome-style QC selection including CheckM2, QUAST, ANI, Mash, and optional taxonomy checks.",
        "modules": ["quick_fasta", "checkm2", "quast", "ani", "mash"],
        "run_mode": "handoff",
    },
}


def list_quality_modules() -> list[dict[str, Any]]:
    return [module.to_dict() for module in QUALITY_MODULES]


def module_keys() -> set[str]:
    return {module.key for module in QUALITY_MODULES}


def external_module_keys() -> set[str]:
    return {module.key for module in QUALITY_MODULES if module.requires_external_tool}


def quality_profile(profile_key: str | None) -> dict[str, Any]:
    key = (profile_key or "quick").strip().lower()
    profile = QUALITY_PROFILES.get(key) or QUALITY_PROFILES["quick"]
    return {"key": key if key in QUALITY_PROFILES else "quick", **profile}


def _source_get(source: Any, key: str, default: Any = None) -> Any:
    if hasattr(source, "get"):
        return source.get(key, default)
    return default


def _source_getlist(source: Any, key: str) -> list[Any]:
    if hasattr(source, "getlist"):
        return list(source.getlist(key))
    value = _source_get(source, key)
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _optional_float(source: Any, key: str, default: float | None) -> float | None:
    raw = _source_get(source, key)
    if raw in {None, ""}:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _optional_int(source: Any, key: str, default: int | None) -> int | None:
    raw = _source_get(source, key)
    if raw in {None, ""}:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def build_quality_config(source: Any) -> dict[str, Any]:
    profile = quality_profile(_source_get(source, "quality_profile"))
    selected_modules = [str(value).strip() for value in _source_getlist(source, "quality_module") if str(value).strip()]
    valid_keys = module_keys()
    if not selected_modules:
        selected_modules = list(profile["modules"])
    selected_modules = [key for key in selected_modules if key in valid_keys]
    if profile["key"] != "quick" and selected_modules == ["quick_fasta"]:
        selected_modules = list(profile["modules"])
    if "quick_fasta" not in selected_modules:
        selected_modules.insert(0, "quick_fasta")

    requested_run_mode = str(_source_get(source, "quality_run_mode", profile["run_mode"]) or profile["run_mode"]).strip().lower()
    if requested_run_mode not in {"quick", "handoff", "nextflow"}:
        requested_run_mode = profile["run_mode"]
    if requested_run_mode == "quick" and profile["key"] != "quick":
        requested_run_mode = profile["run_mode"]

    qc_filter_mode = str(_source_get(source, "qc_filter_mode", DEFAULT_QC_FILTER_MODE) or DEFAULT_QC_FILTER_MODE).strip().lower()
    if qc_filter_mode not in {"review_all", "strict_pass"}:
        qc_filter_mode = DEFAULT_QC_FILTER_MODE
    taxonomy_match_rank = str(_source_get(source, "taxonomy_match_rank", "genus") or "genus").strip().lower()
    if taxonomy_match_rank not in {"genus", "species"}:
        taxonomy_match_rank = "genus"

    thresholds = {
        "min_completeness": _optional_float(source, "qc_min_completeness", DEFAULT_QUALITY_THRESHOLDS["min_completeness"]),
        "max_contamination": _optional_float(source, "qc_max_contamination", DEFAULT_QUALITY_THRESHOLDS["max_contamination"]),
        "max_n_percent": _optional_float(source, "qc_max_n_percent", DEFAULT_QUALITY_THRESHOLDS["max_n_percent"]),
        "max_contigs": _optional_int(source, "qc_max_contigs", DEFAULT_QUALITY_THRESHOLDS["max_contigs"]),
        "min_n50": _optional_int(source, "qc_min_n50", DEFAULT_QUALITY_THRESHOLDS["min_n50"]),
        "min_ani_percent": _optional_float(source, "qc_min_ani_percent", DEFAULT_QUALITY_THRESHOLDS["min_ani_percent"]),
        "max_mash_distance": _optional_float(source, "qc_max_mash_distance", DEFAULT_QUALITY_THRESHOLDS["max_mash_distance"]),
    }

    return {
        "profile": profile,
        "run_mode": requested_run_mode,
        "selected_modules": selected_modules,
        "qc_filter_mode": qc_filter_mode,
        "qc_filter_enabled": qc_filter_mode == "strict_pass",
        "taxonomy_match_rank": taxonomy_match_rank,
        "thresholds": thresholds,
        "external_modules": [key for key in selected_modules if key in external_module_keys()],
    }
