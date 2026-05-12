from __future__ import annotations

import json
import os
import shlex
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .schemas import QUALITY_MODULES


EXTERNAL_TOOL_DIR = Path(__file__).resolve().parent
DEFAULT_NEXTFLOW_REPO = "Tasnimul-Arabi-Anik/PanResistome"
DEFAULT_NEXTFLOW_CONFIG = EXTERNAL_TOOL_DIR / "panresistome_qc" / "fetchm_web_qc.config"
DEFAULT_QUALITY_THREADS = "32"
DEFAULT_CHECKM2_THREADS = "16"


def _path_exists(value: str) -> bool:
    return bool(value) and Path(value).exists()


def _dir_size_bytes(value: str) -> int | None:
    if not value:
        return None
    path = Path(value)
    if not path.exists() or not path.is_dir():
        return None
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def _gtdbtk_data_ready(value: str) -> bool:
    if not value:
        return False
    path = Path(value)
    if not path.exists() or not path.is_dir():
        return False
    required_entries = ["markers", "masks", "metadata", "mrca_red", "msa", "pplacer", "radii", "taxonomy"]
    return all((path / entry).exists() for entry in required_entries)


def quality_tool_status() -> dict[str, Any]:
    tools = {
        "nextflow": shutil.which("nextflow"),
        "checkm2": shutil.which("checkm2"),
        "quast.py": shutil.which("quast.py"),
        "skani": shutil.which("skani"),
        "mash": shutil.which("mash"),
        "gtdbtk": shutil.which("gtdbtk"),
    }
    nextflow_enabled = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
    workflow = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_WORKFLOW", DEFAULT_NEXTFLOW_REPO).strip() or DEFAULT_NEXTFLOW_REPO
    nextflow_config = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_CONFIG", str(DEFAULT_NEXTFLOW_CONFIG)).strip()
    nextflow_profile = os.environ.get("FETCHM_WEBAPP_QUALITY_NEXTFLOW_PROFILE", "conda").strip() or "conda"
    checkm2_db = os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_DB", "").strip()
    checkm2_db_dir = os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_DB_DIR", "").strip()
    gtdbtk_data_path = os.environ.get("FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH", "").strip()
    quality_threads = os.environ.get("FETCHM_WEBAPP_QUALITY_THREADS", DEFAULT_QUALITY_THREADS).strip() or DEFAULT_QUALITY_THREADS
    checkm2_threads = os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_THREADS", DEFAULT_CHECKM2_THREADS).strip() or DEFAULT_CHECKM2_THREADS
    nextflow_syntax_parser = os.environ.get("NXF_SYNTAX_PARSER", "v1").strip() or "v1"
    conda_env_cache_dir = os.environ.get("NXF_CONDA_CACHEDIR", "").strip()
    gtdbtk_data_ready = _gtdbtk_data_ready(gtdbtk_data_path)
    workflow_exists = Path(workflow).exists() if workflow.startswith("/") else None
    nextflow_config_exists = Path(nextflow_config).exists() if nextflow_config else False
    managed_runtime_ready = bool(
        nextflow_enabled
        and tools["nextflow"]
        and shutil.which("conda")
        and (workflow_exists is not False)
        and nextflow_config_exists
    )
    nextflow_managed_tools = {
        "checkm2": bool(managed_runtime_ready and (checkm2_db or checkm2_db_dir)),
        "quast.py": managed_runtime_ready,
        "skani": managed_runtime_ready,
        "mash": managed_runtime_ready,
        "gtdbtk": bool(managed_runtime_ready and gtdbtk_data_ready),
    }
    return {
        "nextflow_enabled": nextflow_enabled,
        "nextflow_available": bool(tools["nextflow"]),
        "conda_available": bool(shutil.which("conda")),
        "managed_runtime_ready": managed_runtime_ready,
        "nextflow_workflow": workflow,
        "nextflow_workflow_exists": workflow_exists,
        "nextflow_config": nextflow_config,
        "nextflow_config_exists": nextflow_config_exists,
        "nextflow_profile": nextflow_profile,
        "nextflow_syntax_parser": nextflow_syntax_parser,
        "quality_threads": quality_threads,
        "checkm2_threads": checkm2_threads,
        "checkm2_db": checkm2_db,
        "checkm2_db_exists": _path_exists(checkm2_db),
        "checkm2_db_dir": checkm2_db_dir,
        "checkm2_db_dir_exists": _path_exists(checkm2_db_dir),
        "checkm2_db_dir_size_bytes": _dir_size_bytes(checkm2_db_dir),
        "gtdbtk_data_path": gtdbtk_data_path,
        "gtdbtk_data_path_exists": _path_exists(gtdbtk_data_path),
        "gtdbtk_data_ready": gtdbtk_data_ready,
        "conda_env_cache_dir": conda_env_cache_dir,
        "conda_env_cache_exists": _path_exists(conda_env_cache_dir),
        "tools": tools,
        "available_tools": {key: bool(value) for key, value in tools.items()},
        "nextflow_managed_tools": nextflow_managed_tools,
        "external_tool_dir": str(EXTERNAL_TOOL_DIR),
    }


def gtdbtk_runtime_ready(status: dict[str, Any] | None = None) -> bool:
    """Return true only when GTDB-Tk can run with reference data."""
    status = status or quality_tool_status()
    return bool(
        (
            status.get("available_tools", {}).get("gtdbtk")
            and status.get("gtdbtk_data_ready")
        )
        or status.get("nextflow_managed_tools", {}).get("gtdbtk")
    )


def validate_quality_runtime(config: dict[str, Any], status: dict[str, Any] | None = None) -> list[str]:
    """Return blocking runtime problems for the selected QC configuration."""
    status = status or quality_tool_status()
    selected_modules = set(str(module) for module in (config.get("selected_modules") or []))
    run_mode = str(config.get("run_mode") or "quick")
    errors: list[str] = []

    if run_mode == "nextflow":
        if not status.get("nextflow_enabled"):
            errors.append("Nextflow execution is disabled on this server.")
        if not status.get("nextflow_available"):
            errors.append("Nextflow is not available in the application runtime.")
        if not status.get("conda_available"):
            errors.append("Conda is not available for the managed Nextflow profile.")
        if not status.get("nextflow_config_exists"):
            errors.append("The FetchM Web QC Nextflow config file is missing.")
        if status.get("nextflow_workflow_exists") is False:
            errors.append("The configured Nextflow workflow path does not exist.")

    if "gtdbtk" in selected_modules and not gtdbtk_runtime_ready(status):
        errors.append(
            "GTDB-Tk taxonomy check requires a complete configured GTDB reference directory. "
            "Set FETCHM_WEBAPP_QUALITY_GTDBTK_DATA_PATH to an extracted GTDB-Tk data path."
        )

    return errors


def build_quality_display_command(input_path: Path, output_dir: Path, config: dict[str, Any]) -> list[str]:
    thresholds = config.get("thresholds") or {}
    command = [
        "fetchm-web-quality-check",
        "--input",
        str(input_path),
        "--outdir",
        str(output_dir),
        "--run-mode",
        str(config.get("run_mode") or "quick"),
    ]
    for module in config.get("selected_modules") or []:
        command.extend(["--module", str(module)])
    for key, value in thresholds.items():
        if value is None:
            continue
        command.extend([f"--{key.replace('_', '-')}", str(value)])
    return command


def prepare_panresistome_local_samples(input_path: Path, output_dir: Path) -> Path:
    sample_dir = output_dir / "external_tools" / "quality_check" / "local_samples" / "fetchm_web_qc"
    sequence_dir = sample_dir / "sequence"
    metadata_dir = sample_dir / "metadata_output"
    sequence_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    if input_path.exists():
        shutil.copy2(input_path, metadata_dir / "ncbi_clean.csv")
        shutil.copy2(input_path, metadata_dir / "fetchm2_clean.csv")

    for fasta_path in sorted(output_dir.glob("*.fna")):
        target = sequence_dir / fasta_path.name
        if target.exists():
            continue
        try:
            os.link(fasta_path, target)
        except OSError:
            shutil.copy2(fasta_path, target)

    (sample_dir / "metadata_engine.txt").write_text("fetchm_web\n", encoding="utf-8")
    return sample_dir.parent


def build_nextflow_command(input_path: Path, output_dir: Path, config: dict[str, Any]) -> list[str]:
    status = quality_tool_status()
    runtime_errors = validate_quality_runtime(config, status)
    if runtime_errors:
        raise RuntimeError(" ".join(runtime_errors))
    workflow = status["nextflow_workflow"]
    thresholds = config.get("thresholds") or {}
    selected_modules = set(str(module) for module in (config.get("selected_modules") or []))
    local_samples = prepare_panresistome_local_samples(input_path, output_dir)
    work_dir = output_dir / "external_tools" / "quality_check" / "nextflow_work"
    command = [
        "nextflow",
        "run",
        workflow,
    ]
    if status.get("nextflow_config") and status.get("nextflow_config_exists"):
        command.extend(["-c", str(status["nextflow_config"])])
    command.extend(
        [
            "-profile",
            status["nextflow_profile"],
            "-work-dir",
            str(work_dir),
        ]
    )
    command.extend([
        "--input",
        str(input_path),
        "--local_samples",
        str(local_samples),
        "--outdir",
        str(output_dir / "nextflow_qc"),
        "--stop_after_qc",
        "true",
        "--qc_filter",
        "true",
        "--sequence_qc_engine",
        "python",
        "--threads",
        os.environ.get("FETCHM_WEBAPP_QUALITY_THREADS", DEFAULT_QUALITY_THREADS),
        "--checkm2_threads",
        os.environ.get("FETCHM_WEBAPP_QUALITY_CHECKM2_THREADS", DEFAULT_CHECKM2_THREADS),
    ])
    module_flags = {
        "checkm2": "--run_checkm2",
        "quast": "--run_quast",
        "ani": "--run_ani",
        "mash": "--run_mash",
        "gtdbtk": "--run_gtdbtk",
    }
    for module, flag in module_flags.items():
        command.extend([flag, "true" if module in selected_modules else "false"])
    if "ani" in selected_modules:
        command.extend(["--ani_tool", os.environ.get("FETCHM_WEBAPP_QUALITY_ANI_TOOL", "skani")])
    if status.get("checkm2_db"):
        command.extend(["--checkm2_db", str(status["checkm2_db"])])
        command.extend(["--checkm2_auto_download_db", "false"])
    elif status.get("checkm2_db_dir"):
        command.extend(["--checkm2_db_dir", str(status["checkm2_db_dir"])])
        command.extend(["--checkm2_auto_download_db", "true"])
    if status.get("gtdbtk_data_path"):
        command.extend(["--gtdbtk_data_path", str(status["gtdbtk_data_path"])])
    threshold_flags = {
        "min_completeness": "--min_completeness",
        "max_contamination": "--max_contamination",
        "max_contigs": "--max_contigs",
        "min_n50": "--min_n50",
        "min_ani_percent": "--ani_species_threshold",
    }
    for key, flag in threshold_flags.items():
        value = thresholds.get(key)
        if value is not None:
            command.extend([flag, str(value)])
    return command


def module_manifest(config: dict[str, Any]) -> list[dict[str, Any]]:
    module_lookup = {module.key: module for module in QUALITY_MODULES}
    selected = set(config.get("selected_modules") or [])
    status = quality_tool_status()
    items: list[dict[str, Any]] = []
    for key in config.get("selected_modules") or []:
        module = module_lookup.get(str(key))
        if module is None:
            continue
        tool_available = True
        if module.requires_external_tool:
            if module.key == "gtdbtk":
                tool_available = gtdbtk_runtime_ready(status)
            else:
                tool_available = bool(status["available_tools"].get(module.tool_name)) or bool(
                    status["managed_runtime_ready"]
                )
        items.append(
            {
                **module.to_dict(),
                "selected": module.key in selected,
                "tool_available": tool_available,
                "execution_status": "built_in" if not module.requires_external_tool else "requires_nextflow",
            }
        )
    return items


def build_quality_handoff(job_id: str, input_path: Path, output_dir: Path, config: dict[str, Any]) -> dict[str, Any]:
    handoff_dir = output_dir / "external_tools" / "quality_check"
    handoff_dir.mkdir(parents=True, exist_ok=True)
    display_command = build_quality_display_command(input_path, output_dir, config)
    nextflow_command = build_nextflow_command(input_path, output_dir, config)
    status = quality_tool_status()
    manifest = {
        "job_id": job_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "output_dir": str(output_dir),
        "handoff_dir": str(handoff_dir),
        "quality_config": config,
        "module_manifest": module_manifest(config),
        "tool_status": status,
        "display_command": display_command,
        "nextflow_command": nextflow_command,
        "nextflow_execution_enabled": bool(status["nextflow_enabled"]),
    }
    (handoff_dir / "quality_check_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (handoff_dir / "nextflow_command.sh").write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nexport NXF_SYNTAX_PARSER=\"${NXF_SYNTAX_PARSER:-v1}\"\n"
        + (
            f"cd {shlex.quote(str(status['nextflow_workflow']))}\n"
            if status.get("nextflow_workflow_exists") is True
            else ""
        )
        + shlex.join(nextflow_command)
        + "\n",
        encoding="utf-8",
    )
    (handoff_dir / "README.md").write_text(
        "\n".join(
            [
                "# FetchM Web External Quality Check Handoff",
                "",
                "This folder records the selected comprehensive QC modules and the Nextflow command",
                "needed to execute PanResistome-style quality checks when the external tool stack",
                "is configured on the server.",
                "",
                "FetchM Web always keeps raw metadata and built-in quick QC outputs separate from",
                "external-tool handoff files so runs remain auditable.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest
