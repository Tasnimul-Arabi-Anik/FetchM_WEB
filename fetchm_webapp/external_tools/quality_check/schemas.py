from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


DEFAULT_QUALITY_THRESHOLDS: dict[str, float | int | None] = {
    "min_completeness": None,
    "max_contamination": None,
    "max_n_percent": 5.0,
    "max_contigs": None,
    "min_n50": None,
    "min_total_bp": None,
    "max_total_bp": None,
    "min_gc_percent": None,
    "max_gc_percent": None,
    "min_ani_percent": None,
    "max_mash_distance": None,
}

STANDARD_QUALITY_THRESHOLDS: dict[str, float | int | None] = {
    **DEFAULT_QUALITY_THRESHOLDS,
    "min_completeness": 90.0,
    "max_contamination": 5.0,
    "min_ani_percent": None,
}

COMPREHENSIVE_QUALITY_THRESHOLDS: dict[str, float | int | None] = {
    **STANDARD_QUALITY_THRESHOLDS,
    "min_ani_percent": 95.0,
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
        "description": "Fast built-in FASTA statistics. Existing completeness/contamination metadata can be enabled when needed.",
        "modules": ["quick_fasta"],
        "run_mode": "quick",
        "thresholds": DEFAULT_QUALITY_THRESHOLDS,
    },
    "standard": {
        "label": "Standard QC",
        "description": "Recommended genome-quality QC with CheckM2 completeness/contamination and QUAST assembly metrics.",
        "modules": ["quick_fasta", "checkm2", "quast"],
        "run_mode": "handoff",
        "thresholds": STANDARD_QUALITY_THRESHOLDS,
    },
    "advanced": {
        "label": "Advanced QC",
        "description": "Second-stage ANI consistency with optional Mash and GTDB-Tk checks on a filtered QC subset.",
        "modules": ["quick_fasta", "ani"],
        "optional_modules": ["mash", "gtdbtk"],
        "run_mode": "handoff",
        "thresholds": COMPREHENSIVE_QUALITY_THRESHOLDS,
    },
    "species_screen": {
        "label": "Advanced QC",
        "description": "Legacy alias for second-stage ANI consistency checks.",
        "modules": ["quick_fasta", "ani"],
        "run_mode": "handoff",
        "thresholds": COMPREHENSIVE_QUALITY_THRESHOLDS,
    },
    "comprehensive": {
        "label": "Advanced QC",
        "description": "Legacy alias for second-stage ANI/Mash/GTDB-Tk checks.",
        "modules": ["quick_fasta", "ani"],
        "optional_modules": ["mash", "gtdbtk"],
        "run_mode": "handoff",
        "thresholds": COMPREHENSIVE_QUALITY_THRESHOLDS,
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


def quality_profile_thresholds(profile: dict[str, Any]) -> dict[str, float | int | None]:
    thresholds = profile.get("thresholds") or DEFAULT_QUALITY_THRESHOLDS
    return dict(thresholds)


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
    default_thresholds = quality_profile_thresholds(profile)
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

    use_existing_checkm = _source_get(source, "qc_use_existing_checkm")
    if profile["key"] == "quick" and not use_existing_checkm:
        default_thresholds["min_completeness"] = None
        default_thresholds["max_contamination"] = None

    thresholds = {
        "min_completeness": _optional_float(source, "qc_min_completeness", default_thresholds["min_completeness"]),
        "max_contamination": _optional_float(source, "qc_max_contamination", default_thresholds["max_contamination"]),
        "max_n_percent": _optional_float(source, "qc_max_n_percent", default_thresholds["max_n_percent"]),
        "max_contigs": _optional_int(source, "qc_max_contigs", default_thresholds["max_contigs"]),
        "min_n50": _optional_int(source, "qc_min_n50", default_thresholds["min_n50"]),
        "min_total_bp": _optional_int(source, "qc_min_total_bp", default_thresholds["min_total_bp"]),
        "max_total_bp": _optional_int(source, "qc_max_total_bp", default_thresholds["max_total_bp"]),
        "min_gc_percent": _optional_float(source, "qc_min_gc_percent", default_thresholds["min_gc_percent"]),
        "max_gc_percent": _optional_float(source, "qc_max_gc_percent", default_thresholds["max_gc_percent"]),
        "min_ani_percent": _optional_float(source, "qc_min_ani_percent", default_thresholds["min_ani_percent"]),
        "max_mash_distance": _optional_float(source, "qc_max_mash_distance", default_thresholds["max_mash_distance"]),
    }

    return {
        "profile": profile,
        "run_mode": requested_run_mode,
        "selected_modules": selected_modules,
        "decision_mode": "pass_fail",
        "qc_filter_mode": qc_filter_mode,
        "qc_filter_enabled": qc_filter_mode == "strict_pass",
        "taxonomy_match_rank": taxonomy_match_rank,
        "thresholds": thresholds,
        "external_modules": [key for key in selected_modules if key in external_module_keys()],
        "use_existing_checkm": bool(use_existing_checkm),
    }
